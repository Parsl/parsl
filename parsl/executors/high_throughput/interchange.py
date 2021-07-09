#!/usr/bin/env python
import argparse
import functools
import zmq
import os
import sys
import platform
import random
import time
import datetime
import pickle
import logging
import queue
import threading
import json

from parsl.utils import setproctitle
from parsl.version import VERSION as PARSL_VERSION
from parsl.serialize import ParslSerializer
serialize_object = ParslSerializer().serialize

from parsl.app.errors import RemoteExceptionWrapper
from parsl.monitoring.message_type import MessageType
from parsl.process_loggers import wrap_with_logs


HEARTBEAT_CODE = (2 ** 32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps((2 ** 32) - 1)


class ShutdownRequest(Exception):
    ''' Exception raised when any async component receives a ShutdownRequest
    '''
    def __init__(self):
        self.tstamp = time.time()

    def __repr__(self):
        return "Shutdown request received at {}".format(self.tstamp)

    def __str__(self):
        return self.__repr__()


class ManagerLost(Exception):
    ''' Task lost due to manager loss. Manager is considered lost when multiple heartbeats
    have been missed.
    '''
    def __init__(self, manager_id, hostname):
        self.manager_id = manager_id
        self.tstamp = time.time()
        self.hostname = hostname

    def __repr__(self):
        return "Task failure due to loss of manager {} on host {}".format(self.manager_id.decode(), self.hostname)

    def __str__(self):
        return self.__repr__()


class BadRegistration(Exception):
    ''' A new Manager tried to join the executor with a BadRegistration message
    '''
    def __init__(self, worker_id, critical=False):
        self.worker_id = worker_id
        self.tstamp = time.time()
        self.handled = "critical" if critical else "suppressed"

    def __repr__(self):
        return "Manager {} attempted to register with a bad registration message. Caused a {} failure".format(
            self.worker_id,
            self.handled)

    def __str__(self):
        return self.__repr__()


class VersionMismatch(Exception):
    ''' Manager and Interchange versions do not match
    '''
    def __init__(self, interchange_version, manager_version):
        self.interchange_version = interchange_version
        self.manager_version = manager_version

    def __repr__(self):
        return "Manager version info {} does not match interchange version info {}, causing a critical failure".format(
            self.interchange_version,
            self.manager_version)

    def __str__(self):
        return self.__repr__()


@functools.total_ordering
class PriorityQueueEntry:
    """ This class is needed because msg will be a dict, and dicts are not
    comparable to each other (and if they were, this would be an unnecessary
    expense because the queue only cares about priority). It provides
    ordering of the priority ignoring the message content, and implements an
    ordering that places None behind all other orderings, for use as a default
    value"""
    def __init__(self, pri, msg):
        self.pri = pri
        self.msg = msg

    def __eq__(self, other):
        if type(self) != type(other):
            return NotImplemented
        return self.pri == other.pri

    def __lt__(self, other):
        # this is deliberately inverted, so that largest priority number comes out of the queue first
        if type(self) != type(other):
            return NotImplemented
        if self.pri is None:  # special case so that None is always less than every other value
            return False  # we are more than populated priorities, and equal to None, the inverse of <
        elif self.pri is not None and other.pri is None:
            return True
        else:  # self/other both not None
            c = self.pri.__gt__(other.pri)
            if c == NotImplemented:
                raise RuntimeError("priority values are not comparable: {} vs {}".format(self.pri, other.pri))
            return c


class Interchange(object):
    """ Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers

    TODO: We most likely need a PUB channel to send out global commands, like shutdown
    """
    def __init__(self,
                 client_address="127.0.0.1",
                 interchange_address="127.0.0.1",
                 client_ports=(50055, 50056, 50057),
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 hub_address=None,
                 hub_port=None,
                 heartbeat_threshold=60,
                 logdir=".",
                 logging_level=logging.INFO,
                 poll_period=10,
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
             This is overridden when the worker_ports option is set. Default: (54000, 55000)

        hub_address : str
             The ip address at which the interchange can send info about managers to when monitoring is enabled.
             This is passed via dfk and executor automatically. Default: None (meaning monitoring disabled)

        hub_port : str
             The port at which the interchange can send info about managers to when monitoring is enabled.
             This is passed via dfk and executor automatically. Default: None (meaning monitoring disabled)

        heartbeat_threshold : int
             Number of seconds since the last heartbeat after which worker is considered lost.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO (20)

        poll_period : int
             The main thread polling period, in milliseconds. Default: 10ms

        """
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        start_file_logger("{}/interchange.log".format(self.logdir), level=logging_level)
        logger.debug("Initializing Interchange process")

        self.client_address = client_address
        self.interchange_address = interchange_address
        self.poll_period = poll_period

        logger.info("Attempting connection to client at {} on ports: {},{},{}".format(
            client_address, client_ports[0], client_ports[1], client_ports[2]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)

        # this controls the speed at which the task incoming queue loop runs. The only thing
        # that loop does aside from task_incoming is check for kill event. The default of
        # 10ms is pretty high - for this project, I'm fine with this taking a second or so to
        # detect a kill event.
        self.task_incoming.RCVTIMEO = 5000  # in milliseconds
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))

        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))

        self.command_channel = self.context.socket(zmq.REP)
        self.command_channel.RCVTIMEO = 1000  # in milliseconds
        self.command_channel.connect("tcp://{}:{}".format(client_address, client_ports[2]))
        logger.info("Connected to client")

        self.hub_address = hub_address
        self.hub_port = hub_port

        self.pending_task_queue = queue.PriorityQueue(maxsize=10 ** 6)

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

        self._ready_manager_queue = {}

        self.heartbeat_threshold = heartbeat_threshold

        self.current_platform = {'parsl_v': PARSL_VERSION,
                                 'python_v': "{}.{}.{}".format(sys.version_info.major,
                                                               sys.version_info.minor,
                                                               sys.version_info.micro),
                                 'os': platform.system(),
                                 'hostname': platform.node(),
                                 'dir': os.getcwd()}

        logger.info("Platform info: {}".format(self.current_platform))

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
                qe = self.pending_task_queue.get(block=False)
            except queue.Empty:
                break
            else:
                tasks.append(qe.msg)

        return tasks

    @wrap_with_logs(target="interchange")
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
                logger.debug("[TASK_PULL_THREAD] No task received from task_incoming zmq queue. {} tasks already in internal queue".format(
                    self.pending_task_queue.qsize()))
                continue

            if msg == 'STOP':
                kill_event.set()
                break
            else:
                self.pending_task_queue.put(PriorityQueueEntry(msg['priority'], msg))
                task_counter += 1
                logger.debug("[TASK_PULL_THREAD] Fetched task:{}".format(task_counter))

    def _create_monitoring_channel(self):
        if self.hub_address and self.hub_port:
            logger.info("Connecting to monitoring")
            hub_channel = self.context.socket(zmq.DEALER)
            hub_channel.set_hwm(0)
            hub_channel.connect("tcp://{}:{}".format(self.hub_address, self.hub_port))
            logger.info("Monitoring enabled and connected to hub")
            return hub_channel
        else:
            return None

    def _send_monitoring_info(self, hub_channel, manager):
        if hub_channel:
            logger.info("Sending message {} to hub".format(self._ready_manager_queue[manager]))

            d = self._ready_manager_queue[manager].copy()
            d['timestamp'] = datetime.datetime.now()
            d['last_heartbeat'] = datetime.datetime.fromtimestamp(d['last_heartbeat'])

            hub_channel.send_pyobj((MessageType.NODE_INFO, d))

    @wrap_with_logs(target="interchange")
    def _command_server(self, kill_event):
        """ Command server to run async command to the interchange
        """
        logger.debug("[COMMAND] Command Server Starting")

        # Need to create a new ZMQ socket for command server thread
        hub_channel = self._create_monitoring_channel()

        while not kill_event.is_set():
            try:
                command_req = self.command_channel.recv_pyobj()
                logger.debug("[COMMAND] Received command request: {}".format(command_req))
                if command_req == "OUTSTANDING_C":
                    outstanding = self.pending_task_queue.qsize()
                    for manager in self._ready_manager_queue:
                        outstanding += len(self._ready_manager_queue[manager]['tasks'])
                    reply = outstanding

                elif command_req == "WORKERS":
                    num_workers = 0
                    for manager in self._ready_manager_queue:
                        num_workers += self._ready_manager_queue[manager]['worker_count']
                    reply = num_workers

                elif command_req == "MANAGERS":
                    reply = []
                    for manager in self._ready_manager_queue:
                        idle_duration = 0
                        if self._ready_manager_queue[manager]['idle_since'] is not None:
                            idle_duration = time.time() - self._ready_manager_queue[manager]['idle_since']
                        resp = {'manager': manager.decode('utf-8'),
                                'block_id': self._ready_manager_queue[manager]['block_id'],
                                'worker_count': self._ready_manager_queue[manager]['worker_count'],
                                'tasks': len(self._ready_manager_queue[manager]['tasks']),
                                'idle_duration': idle_duration,
                                'active': self._ready_manager_queue[manager]['active']}
                        reply.append(resp)

                elif command_req.startswith("HOLD_WORKER"):
                    cmd, s_manager = command_req.split(';')
                    manager = s_manager.encode('utf-8')
                    logger.info("[CMD] Received HOLD_WORKER for {}".format(manager))
                    if manager in self._ready_manager_queue:
                        self._ready_manager_queue[manager]['active'] = False
                        reply = True
                        self._send_monitoring_info(hub_channel, manager)
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

    @wrap_with_logs
    def start(self):
        """ Start the interchange

        Parameters:
        ----------

        TODO: Move task receiving to a thread
        """
        logger.info("Incoming ports bound")

        hub_channel = self._create_monitoring_channel()

        # poll period is never specified as a start() parameter, so removing the defaulting here as noise.
        # poll_period = self.poll_period
        # however for my hacking:
        poll_period = 1000
        # because the executor level poll period also changes the worker pool poll period setting, which I want to experiment with separately.
        # This setting reduces the speed at which the interchange main loop
        # iterates. It will iterate once per this tmie, or when two of the
        # three queues that we need to check are interesting. which means that
        # third queue (pending_task_queue) will only be dispatched on once
        # every poll_period. although everythign waiting will be dispatched
        # then. this will reduce speed of task dispatching some, but give
        # much less log output. I wonder if it is possible to make this detectable
        # using poll too (it's a python queue, not a zmq queue which the other poll is for)

        start = time.time()
        count = 0

        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(target=self.migrate_tasks_to_internal,
                                                    args=(self._kill_event,),
                                                    name="Interchange-Task-Puller")
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(target=self._command_server,
                                                args=(self._kill_event,),
                                                name="Interchange-Command")
        self._command_thread.start()

        poller = zmq.Poller()
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        # These are managers which we should examine in an iteration
        # for scheduling a job (or maybe any other attention?).
        # Anything altering the state of the manager should add it
        # onto this list.
        interesting_managers = set()

        while not self._kill_event.is_set():
            logger.debug("BENC: starting poll")
            self.socks = dict(poller.poll(timeout=poll_period))
            logger.debug("BENC: ending poll")

            # Listen for requests for work
            if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
                logger.debug("[MAIN] starting task_outgoing section")
                message = self.task_outgoing.recv_multipart()
                manager = message[0]

                if manager not in self._ready_manager_queue:
                    reg_flag = False

                    try:
                        msg = json.loads(message[1].decode('utf-8'))
                        reg_flag = True
                    except Exception:
                        logger.warning("[MAIN] Got Exception reading registration message from manager: {}".format(
                            manager), exc_info=True)
                        logger.debug("[MAIN] Message :\n{}\n".format(message[1]))
                    else:
                        # We set up an entry only if registration works correctly
                        self._ready_manager_queue[manager] = {'last_heartbeat': time.time(),
                                                              'idle_since': time.time(),
                                                              'free_capacity': 0,
                                                              'block_id': None,
                                                              'max_capacity': 0,
                                                              'worker_count': 0,
                                                              'active': True,
                                                              'tasks': []}
                    if reg_flag is True:
                        interesting_managers.add(manager)
                        logger.info("[MAIN] Adding manager: {} to ready queue".format(manager))
                        self._ready_manager_queue[manager].update(msg)
                        logger.info("[MAIN] Registration info for manager {}: {}".format(manager, msg))
                        self._send_monitoring_info(hub_channel, manager)

                        if (msg['python_v'].rsplit(".", 1)[0] != self.current_platform['python_v'].rsplit(".", 1)[0] or
                            msg['parsl_v'] != self.current_platform['parsl_v']):
                            logger.warning("[MAIN] Manager {} has incompatible version info with the interchange".format(manager))
                            logger.debug("Setting kill event")
                            self._kill_event.set()
                            e = VersionMismatch("py.v={} parsl.v={}".format(self.current_platform['python_v'].rsplit(".", 1)[0],
                                                                            self.current_platform['parsl_v']),
                                                "py.v={} parsl.v={}".format(msg['python_v'].rsplit(".", 1)[0],
                                                                            msg['parsl_v'])
                            )
                            result_package = {'type': 'result', 'task_id': -1, 'exception': serialize_object(e)}
                            pkl_package = pickle.dumps(result_package)
                            self.results_outgoing.send(pkl_package)
                            logger.warning("[MAIN] Sent failure reports, unregistering manager")
                        else:
                            logger.info("[MAIN] Manager {} has compatible Parsl version {}".format(manager, msg['parsl_v']))
                            logger.info("[MAIN] Manager {} has compatible Python version {}".format(manager,
                                                                                                    msg['python_v'].rsplit(".", 1)[0]))
                    else:
                        # Registration has failed.
                        logger.debug("[MAIN] Suppressing bad registration from manager:{}".format(
                            manager))

                else:
                    tasks_requested = int.from_bytes(message[1], "little")
                    self._ready_manager_queue[manager]['last_heartbeat'] = time.time()
                    if tasks_requested == HEARTBEAT_CODE:
                        logger.debug("[MAIN] Manager {} sent heartbeat".format(manager))
                        self.task_outgoing.send_multipart([manager, b'', PKL_HEARTBEAT_CODE])
                    else:
                        logger.debug("[MAIN] Manager {} requested {} tasks".format(manager, tasks_requested))
                        self._ready_manager_queue[manager]['free_capacity'] = tasks_requested
                        interesting_managers.add(manager)
                logger.debug("[MAIN] leaving task_outgoing section")

            # If we had received any requests, check if there are tasks that could be passed

            logger.debug("Managers count (interesting/total): {interesting}/{total}".format(
                total=len(self._ready_manager_queue),
                interesting=len(interesting_managers)))

            if interesting_managers and not self.pending_task_queue.empty():
                shuffled_managers = list(interesting_managers)
                random.shuffle(shuffled_managers)

                while shuffled_managers and not self.pending_task_queue.empty():  # cf. the if statement above...
                    manager = shuffled_managers.pop()
                    tasks_inflight = len(self._ready_manager_queue[manager]['tasks'])
                    real_capacity = min(self._ready_manager_queue[manager]['free_capacity'],
                                        self._ready_manager_queue[manager]['max_capacity'] - tasks_inflight)

                    if (real_capacity and self._ready_manager_queue[manager]['active']):
                        tasks = self.get_tasks(real_capacity)
                        if tasks:
                            self.task_outgoing.send_multipart([manager, b'', pickle.dumps(tasks)])
                            # after this point, we've sent a task to the manager, but we haven't
                            # added it to the 'task' list for that manager, because we don't
                            # do that for another 5 lines. That should be pretty fast, though?
                            # but we shouldn't try removing it from the tasks list until we have
                            # passed that point anyway?
                            task_count = len(tasks)
                            count += task_count
                            tids = [t['task_id'] for t in tasks]
                            self._ready_manager_queue[manager]['free_capacity'] -= task_count
                            self._ready_manager_queue[manager]['tasks'].extend(tids)
                            self._ready_manager_queue[manager]['idle_since'] = None
                            logger.debug("[MAIN] Sent tasks: {} to manager {}".format(tids, manager))
                            if self._ready_manager_queue[manager]['free_capacity'] > 0:
                                logger.debug("[MAIN] Manager {} has free_capacity {}".format(manager, self._ready_manager_queue[manager]['free_capacity']))
                                # ... so keep it in the interesting_managers list
                            else:
                                logger.debug("[MAIN] Manager {} is now saturated".format(manager))
                                interesting_managers.remove(manager)
                    else:
                        interesting_managers.remove(manager)
                        # logger.debug("Nothing to send to manager {}".format(manager))
                logger.debug("[MAIN] leaving _ready_manager_queue section, with {} managers still interesting".format(len(interesting_managers)))
            else:
                logger.debug("[MAIN] either no interesting managers or no tasks, so skipping manager pass")
            # Receive any results and forward to client
            if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
                logger.debug("[MAIN] entering results_incoming section")
                manager, *all_messages = self.results_incoming.recv_multipart()
                if manager not in self._ready_manager_queue:
                    logger.warning("[MAIN] Received a result from a un-registered manager: {}".format(manager))
                else:
                    logger.debug("[MAIN] Got {} result items in batch".format(len(all_messages)))

                    b_messages = []

                    # this block needs to split messages into 'result' messages, and process as previously;
                    # monitoring messages, which should be sent to monitoring via whatever is used?
                    # and others, which should generate a non-fatal error log

                    # TODO: rework to avoid depickling twice... because that's quite expensive I expect

                    for message in all_messages:
                        r = pickle.loads(message)
                        if r['type'] == 'result':
                            # process this for task ID and forward to executor
                            b_messages.append(message)
                        elif r['type'] == 'monitoring':
                            hub_channel.send_pyobj(r['payload'])
                        else:
                            logger.error("Interchange discarding result_queue message of unknown type: {}".format(r['type']))

                    for b_message in b_messages:
                        r = pickle.loads(b_message)
                        try:
                            self._ready_manager_queue[manager]['tasks'].remove(r['task_id'])
                        except Exception:
                            # If we reach here, there's something very wrong.
                            logger.exception("Ignoring exception removing task_id {} for manager {} with task list {}".format(
                                r['task_id'],
                                manager,
                                self._ready_manager_queue[manager]['tasks']))

                    if b_messages:
                        self.results_outgoing.send_multipart(b_messages)

                    logger.debug("[MAIN] Current tasks: {}".format(self._ready_manager_queue[manager]['tasks']))
                    if len(self._ready_manager_queue[manager]['tasks']) == 0:
                        self._ready_manager_queue[manager]['idle_since'] = time.time()
                logger.debug("[MAIN] leaving results_incoming section")

            bad_managers = [manager for manager in self._ready_manager_queue if
                            time.time() - self._ready_manager_queue[manager]['last_heartbeat'] > self.heartbeat_threshold]
            for manager in bad_managers:
                logger.debug("[MAIN] Last: {} Current: {}".format(self._ready_manager_queue[manager]['last_heartbeat'], time.time()))
                logger.warning("[MAIN] Too many heartbeats missed for manager {}".format(manager))
                if self._ready_manager_queue[manager]['active']:
                    self._ready_manager_queue[manager]['active'] = False
                    self._send_monitoring_info(hub_channel, manager)

                for tid in self._ready_manager_queue[manager]['tasks']:
                    try:
                        raise ManagerLost(manager, self._ready_manager_queue[manager]['hostname'])
                    except Exception:
                        result_package = {'type': 'result', 'task_id': tid, 'exception': serialize_object(RemoteExceptionWrapper(*sys.exc_info()))}
                        pkl_package = pickle.dumps(result_package)
                        self.results_outgoing.send(pkl_package)
                        logger.warning("[MAIN] Sent failure reports, unregistering manager")
                self._ready_manager_queue.pop(manager, 'None')
                if manager in interesting_managers:
                    interesting_managers.remove(manager)

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
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


@wrap_with_logs(target="interchange")
def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function. The args, kwargs match that of the Interchange.__init__
    """
    setproctitle("parsl: HTEX interchange")
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
    parser.add_argument("-p", "--poll_period",
                        help="REQUIRED: poll period used for main thread")
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
