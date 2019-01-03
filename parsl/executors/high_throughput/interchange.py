#!/usr/bin/env python
import argparse
import zmq
# import uuid
import os
import sys
import platform
import random
import time
import pickle
import logging
import queue
import threading
import json

from parsl.version import VERSION as PARSL_VERSION
from ipyparallel.serialize import serialize_object

LOOP_SLOWDOWN = 0.0  # in seconds
HEARTBEAT_CODE = (2 ** 32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps((2 ** 32) - 1)


class ShutdownRequest(Exception):
    ''' Exception raised when any async component receives a ShutdownRequest
    '''
    def __init__(self):
        self.tstamp = time.time()

    def __repr__(self):
        return "Shutdown request received at {}".format(self.tstamp)


class ManagerLost(Exception):
    ''' Task lost due to worker loss. Worker is considered lost when multiple heartbeats
    have been missed.
    '''
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tstamp = time.time()

    def __repr__(self):
        return "Task failure due to loss of worker {}".format(self.worker_id)


class Interchange(object):
    """ Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    TODO: We most likely need a PUB channel to send out global commands, like shutdown
    """
    def __init__(self,
                 client_address="127.0.0.1",
                 interchange_address="127.0.0.1",
                 client_ports=(50055, 50056, 50057),
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 heartbeat_threshold=60,
                 logdir=".",
                 logging_level=logging.INFO,
             ):
        """
        Parameters
        ----------
        client_address : str
             The ip address at which the parsl client can be reached. Default: "127.0.0.1"

        interchange_address : str
             The ip address at which the workers will be able to reach the Interchange. Default: "127.0.0.1"

        client_ports : triple(int, int, int)
             The ports at which the client can be reached

        worker_ports : tuple(int, int)
             The specific two ports at which workers will connect to the Interchange. Default: None

        worker_port_range : tuple(int, int)
             The interchange picks ports at random from the range which will be used by workers.
             This is overridden when the worker_ports option is set. Defauls: (54000, 55000)

        heartbeat_threshold : int
             Number of seconds since the last heartbeat after which worker is considered lost.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO (20)

        """
        self.logdir = logdir
        try:
            os.makedirs(self.logdir)
        except FileExistsError:
            pass

        start_file_logger("{}/interchange.log".format(self.logdir), level=logging_level)
        logger.debug("Initializing Interchange process")

        self.client_address = client_address
        self.interchange_address = interchange_address

        logger.info("Attempting connection to client at {} on ports: {},{},{}".format(
            client_address, client_ports[0], client_ports[1], client_ports[2]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)
        self.task_incoming.RCVTIMEO = 10  # in milliseconds
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))
        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))

        self.command_channel = self.context.socket(zmq.REP)
        self.command_channel.RCVTIMEO = 1000  # in milliseconds
        self.command_channel.connect("tcp://{}:{}".format(client_address, client_ports[2]))
        logger.info("Connected to client")

        self.pending_task_queue = queue.Queue()

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing.set_hwm(0)
        self.results_incoming = self.context.socket(zmq.ROUTER)
        self.results_incoming.set_hwm(0)

        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind("tcp://*:{}".format(self.worker_task_port))
            self.results_incoming.bind("tcp://*:{}".format(self.worker_result_port))

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port('tcp://*',
                                                                           min_port=worker_port_range[0],
                                                                           max_port=worker_port_range[1], max_tries=100)
            self.worker_result_port = self.results_incoming.bind_to_random_port('tcp://*',
                                                                                min_port=worker_port_range[0],
                                                                                max_port=worker_port_range[1], max_tries=100)

        logger.info("Bound to ports {},{} for incoming worker connections".format(
            self.worker_task_port, self.worker_result_port))

        self._task_queue = []
        self._ready_manager_queue = {}
        self.max_task_queue_size = 10 ** 5

        self.heartbeat_threshold = heartbeat_threshold

        self.current_platform = {'parsl_v': PARSL_VERSION,
                                 'python_v': "{}.{}.{}".format(sys.version_info.major,
                                                               sys.version_info.minor,
                                                               sys.version_info.micro),
                                 'os': platform.system(),
                                 'hname': platform.node(),
                                 'dir': os.getcwd()}

    def get_tasks(self, count):
        """ Obtains a batch of tasks from the internal pending_task_queue

        Parameters
        ----------
        count: int
            Count of tasks to get from the queue

        Returns
        -------
        List of upto count tasks. May return fewer than count down to an empty list
            eg. [{'task_id':<x>, 'buffer':<buf>} ... ]
        """
        tasks = []
        for i in range(0, count):
            try:
                x = self.pending_task_queue.get(block=False)
            except queue.Empty:
                break
            else:
                tasks.append(x)

        return tasks

    def migrate_tasks_to_internal(self, kill_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK_PULL_THREAD] Starting")
        task_counter = 0
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        while not kill_event.is_set():
            try:
                msg = self.task_incoming.recv_pyobj()
            except zmq.Again:
                # We just timed out while attempting to receive
                logger.debug("[TASK_PULL_THREAD] {} tasks in internal queue".format(self.pending_task_queue.qsize()))
                continue

            if msg == 'STOP':
                kill_event.set()
                break
            else:
                self.pending_task_queue.put(msg)
                task_counter += 1
                logger.debug("[TASK_PULL_THREAD] Fetched task:{}".format(task_counter))

    def _command_server(self, kill_event):
        """ Command server to run async command to the interchange
        """
        logger.debug("[COMMAND] Command Server Starting")
        while not kill_event.is_set():
            try:
                command_req = self.command_channel.recv_pyobj()
                logger.debug("[COMMAND] Received command request: {}".format(command_req))
                if command_req == "OUTSTANDING_C":
                    outstanding = self.pending_task_queue.qsize()
                    for manager in self._ready_manager_queue:
                        outstanding += len(self._ready_manager_queue[manager]['tasks'])
                    reply = outstanding

                elif command_req == "MANAGERS":
                    reply = []
                    for manager in self._ready_manager_queue:
                        resp = (manager.decode('utf-8'),
                                len(self._ready_manager_queue[manager]['tasks']),
                                self._ready_manager_queue[manager]['active'])
                        reply.append(resp)

                elif command_req.startswith("HOLD_WORKER"):
                    cmd, s_manager = command_req.split(';')
                    manager = s_manager.encode('utf-8')
                    logger.info("[CMD] Received HOLD_WORKER for {}".format(manager))
                    if manager in self._ready_manager_queue:
                        self._ready_manager_queue[manager]['active'] = False
                        reply = True
                    else:
                        reply = False

                elif command_req == "SHUTDOWN":
                    logger.info("[CMD] Received SHUTDOWN command")
                    kill_event.set()
                    reply = True

                else:
                    reply = None

                logger.debug("[COMMAND] Reply: {}".format(reply))
                self.command_channel.send_pyobj(reply)

            except zmq.Again:
                logger.debug("[COMMAND] is alive")
                continue

    def start(self, poll_period=1):
        """ Start the NeedNameQeueu

        Parameters:
        ----------

        poll_period : int
              Poll period in milliseconds

        TODO: Move task receiving to a thread
        """
        logger.info("Incoming ports bound")

        start = time.time()
        count = 0

        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(target=self.migrate_tasks_to_internal,
                                                    args=(self._kill_event,))
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(target=self._command_server,
                                                args=(self._kill_event,))
        self._command_thread.start()

        poller = zmq.Poller()
        # poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        while not self._kill_event.is_set():
            self.socks = dict(poller.poll(timeout=poll_period))

            # Listen for requests for work
            if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
                message = self.task_outgoing.recv_multipart()
                manager = message[0]

                if manager not in self._ready_manager_queue:
                    msg = json.loads(message[1].decode('utf-8'))
                    logger.info("[MAIN] Adding manager: {} to ready queue".format(manager))
                    self._ready_manager_queue[manager] = {'last': time.time(),
                                                          'free_capacity': 0,
                                                          'active': True,
                                                          'tasks': []}
                    self._ready_manager_queue[manager].update(msg)
                    logger.info("Registration info for manager {}: {}".format(manager, msg))
                    if (msg['python_v'] != self.current_platform['python_v'] or
                        msg['parsl_v'] != self.current_platform['parsl_v']):
                        logger.warn("Manager {} has incompatible version info with the interchange".format(manager))
                        logger.debug("Setting kill event")
                        self._kill_event.set()
                        e = ManagerLost(manager)
                        result_package = {'task_id': -1, 'exception': serialize_object(e)}
                        pkl_package = pickle.dumps(result_package)
                        self.results_outgoing.send(pkl_package)
                        logger.warning("[MAIN] Sent failure reports, unregistering manager")

                else:
                    tasks_requested = int.from_bytes(message[1], "little")
                    logger.debug("[MAIN] Manager {} requested {} tasks".format(manager, tasks_requested))
                    self._ready_manager_queue[manager]['last'] = time.time()
                    if tasks_requested == HEARTBEAT_CODE:
                        logger.debug("[MAIN] Manager {} sends heartbeat".format(manager))
                        self.task_outgoing.send_multipart([manager, b'', PKL_HEARTBEAT_CODE])
                    else:
                        self._ready_manager_queue[manager]['free_capacity'] = tasks_requested

            # If we had received any requests, check if there are tasks that could be passed
            logger.debug("Managers: {}".format(self._ready_manager_queue))
            if self._ready_manager_queue:
                shuffled_managers = list(self._ready_manager_queue.keys())
                random.shuffle(shuffled_managers)
                logger.debug("Shuffled: {}".format(shuffled_managers))
                # for manager in self._ready_manager_queue:
                for manager in shuffled_managers:
                    if (self._ready_manager_queue[manager]['free_capacity'] and
                        self._ready_manager_queue[manager]['active']):
                        tasks = self.get_tasks(self._ready_manager_queue[manager]['free_capacity'])
                        if tasks:
                            self.task_outgoing.send_multipart([manager, b'', pickle.dumps(tasks)])
                            task_count = len(tasks)
                            count += task_count
                            tids = [t['task_id'] for t in tasks]
                            logger.debug("[MAIN] Sent tasks: {} to {}".format(tids, manager))
                            self._ready_manager_queue[manager]['free_capacity'] -= task_count
                            self._ready_manager_queue[manager]['tasks'].extend(tids)
                    else:
                        logger.debug("Nothing to send")

            # Receive any results and forward to client
            if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
                manager, *b_messages = self.results_incoming.recv_multipart()
                if manager not in self._ready_manager_queue:
                    logger.warning("[MAIN] Received a result from a un-registered manager: {}".format(manager))
                else:
                    logger.debug("[MAIN] Got {} result items in batch".format(len(b_messages)))
                    for b_message in b_messages:
                        r = pickle.loads(b_message)
                        # logger.debug("[MAIN] Received result for task {} from {}".format(r['task_id'], manager))
                        self._ready_manager_queue[manager]['tasks'].remove(r['task_id'])
                    self.results_outgoing.send_multipart(b_messages)
                    logger.debug("[MAIN] Current tasks: {}".format(self._ready_manager_queue[manager]['tasks']))

            bad_managers = [manager for manager in self._ready_manager_queue if
                            time.time() - self._ready_manager_queue[manager]['last'] > self.heartbeat_threshold]
            for manager in bad_managers:
                logger.debug("[MAIN] Last: {} Current: {}".format(self._ready_manager_queue[manager]['last'], time.time()))
                logger.warning("[MAIN] Too many heartbeats missed for manager {}".format(manager))
                e = ManagerLost(manager)
                for tid in self._ready_manager_queue[manager]['tasks']:
                    result_package = {'task_id': tid, 'exception': serialize_object(e)}
                    pkl_package = pickle.dumps(result_package)
                    self.results_outgoing.send(pkl_package)
                    logger.warning("[MAIN] Sent failure reports, unregistering manager")
                self._ready_manager_queue.pop(manager, 'None')

        delta = time.time() - start
        logger.info("Processed {} tasks in {} seconds".format(count, delta))
        logger.warning("Exiting")


def start_file_logger(filename, name='interchange', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name. Default="parsl.executors.interchange"
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.

    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function. The args, kwargs match that of the Interchange.__init__
    """
    # logger = multiprocessing.get_logger()
    ic = Interchange(*args, **kwargs)
    comm_q.put((ic.worker_task_port,
                ic.worker_result_port))
    ic.start()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client_address",
                        help="Client address")
    parser.add_argument("-l", "--logdir", default="parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-t", "--task_url",
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-r", "--result_url",
                        help="REQUIRED: ZMQ url for posting results")
    parser.add_argument("--worker_ports", default=None,
                        help="OPTIONAL, pair of workers ports to listen on, eg --worker_ports=50001,50005")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")

    args = parser.parse_args()

    # Setup logging
    global logger
    format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger("interchange")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel('DEBUG' if args.debug is True else 'INFO')
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.debug("Starting Interchange")

    optionals = {}
    if args.worker_ports:
        optionals['worker_ports'] = [int(i) for i in args.worker_ports.split(',')]
    ic = Interchange(**optionals)
    ic.start()
