#!/usr/bin/env python
import argparse
import zmq
import uuid
import time
import pickle
import logging
import queue
import threading

from ipyparallel.serialize import serialize_object

LOOP_SLOWDOWN = 0.0  # in seconds


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
                 client_ports=(50055, 50056),
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 heartbeat_period=60,
                 logging_level=logging.INFO,
             ):
        """
        Parameters
        ----------
        client_address : str
             The ip address at which the parsl client can be reached. Default: "127.0.0.1"

        interchange_address : str
             The ip address at which the workers will be able to reach the Interchange. Default: "127.0.0.1"

        client_ports : tuple(int, int)
             The ports at which the client can be reached

        worker_ports : tuple(int, int)
             The specific two ports at which workers will connect to the Interchange. Default: None

        worker_port_range : tuple(int, int)
             The interchange picks ports at random from the range which will be used by workers.
             This is overridden when the worker_ports option is set. Defauls: (54000, 55000)

        heartbeat_period : int
             Heartbeat period expected from workers (seconds). Default: 10s

        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO (20)

        """
        start_file_logger("interchange.log", level=logging_level)
        logger.debug("Initializing Interchange process")

        self.client_address = client_address
        self.interchange_address = interchange_address
        self.identity = uuid.uuid4()

        logger.info("Attempting connection to client at {} on ports: {},{}".format(
            client_address, client_ports[0], client_ports[1]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.RCVTIMEO = 10  # in milliseconds
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))
        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))
        logger.debug("Connected to client")

        self.pending_task_queue = queue.Queue(maxsize=10 ^ 6)

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.results_incoming = self.context.socket(zmq.ROUTER)

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
        self.max_task_queue_size = 10 ^ 5

        self.heartbeat_thresh = heartbeat_period * 2

    def _get_tasks(self, count, socks):
        """ Obtains a batch of tasks from the task queue.

        Parameters
        ----------
        count: int
            Count of tasks to get from the queue
        socks: dict(poll events)
            Dictionary of socket events from zmq.poller.poll()

        Returns
        -------
        List of upto count tasks. May return fewer than count down to an empty list
            eg. [{'task_id':<x>, 'buffer':<buf>} ... ]

        Raises
        ------
        ShutdownRequest: If shutdown requested by client.
            Tasks are moved from Zmq to the internal task_queue only when the queue in not full.
            As a result the 'STOP' request might be queued in ZMQ and may be processed in a
            delayed manner.

        """
        # Listen for tasks
        tasks = []
        logger.debug("[GET_TASKS] Listening for {} tasks".format(count))
        for c in range(count):
            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                if len(self._task_queue) < self.max_task_queue_size:
                    # There's an unpickling cost here, could optimize by prepending
                    # buffer with tid
                    try:
                        msg = self.task_incoming.recv_pyobj()
                    except zmq.Again:
                        # We just timed out while attempting to receive
                        logger.debug("There are no more tasks in the incoming queue. Breaking")
                        break
                    # msg = self.task_incoming.recv_string()
                    if msg == 'STOP':
                        raise ShutdownRequest
                    else:
                        tasks.append(msg)
            else:
                logger.debug("[GET_TASKS] Returning with {} tasks".format(c))
                break

        return tasks

    def get_tasks(self, count, socks):

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
        logger.info("TASK_PULL_THREAD] Starting")
        task_counter = 0
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        while not kill_event.is_set():
            try:
                msg = self.task_incoming.recv_pyobj()
            except zmq.Again:
                # We just timed out while attempting to receive
                continue

            if msg == 'STOP':
                kill_event.set()
                break
            else:
                self.pending_task_queue.put(msg)
                task_counter += 1
                logger.debug("[TASK_PULL_THREAD] Fetched task:{}".format(task_counter))

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

        poller = zmq.Poller()
        # poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        while True:
            self.socks = dict(poller.poll(timeout=poll_period))

            # Listen for requests for work
            if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
                message = self.task_outgoing.recv_multipart()
                manager = message[0]
                tasks_requested = int.from_bytes(message[1], "little")
                manager = int.from_bytes(message[0], "little")

                logger.debug("[MAIN] Manager {} requested {} tasks".format(manager, tasks_requested))
                if manager not in self._ready_manager_queue:
                    logger.info("[MAIN] Adding manager: {} to ready queue".format(manager))
                    self._ready_manager_queue[manager] = {'last': time.time(),
                                                          # [TODO] Add support for tracking walltimes
                                                          # 'wtime': 60,
                                                          'free_capacity': tasks_requested,
                                                          'tasks': []}
                else:
                    self._ready_manager_queue[manager]['last'] = time.time()
                    self._ready_manager_queue[manager]['free_capacity'] = tasks_requested

            # If we had received any requests, check if there are tasks that could be passed
            for manager in self._ready_manager_queue:
                if self._ready_manager_queue[manager]['free_capacity']:
                    tasks = self.get_tasks(self._ready_manager_queue[manager]['free_capacity'], self.socks)
                    if tasks:
                        self.task_outgoing.send_multipart([message[0], b'', pickle.dumps(tasks)])
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
                b_manager, *b_messages = self.results_incoming.recv_multipart()
                manager = int.from_bytes(b_manager, "little")
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
                            time.time() - self._ready_manager_queue[manager]['last'] > self.heartbeat_thresh]
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
        logger("Received {} tasks in {} seconds".format(count, delta))


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

    args = parser.parse_args()

    ic = Interchange()
    ic.start()
