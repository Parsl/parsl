#!/usr/bin/env python
import zmq
import os
import sys
import platform
import random
import time
import datetime
import pickle
import signal
import logging
import queue
import threading
import json

from typing import cast, Any, Dict, Set, Optional

from parsl.utils import setproctitle
from parsl.version import VERSION as PARSL_VERSION
from parsl.serialize import serialize as serialize_object

from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput.manager_record import ManagerRecord
from parsl.monitoring.message_type import MessageType
from parsl.process_loggers import wrap_with_logs


HEARTBEAT_CODE = (2 ** 32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps((2 ** 32) - 1)

LOGGER_NAME = "interchange"
logger = logging.getLogger(LOGGER_NAME)


class ManagerLost(Exception):
    ''' Task lost due to manager loss. Manager is considered lost when multiple heartbeats
    have been missed.
    '''
    def __init__(self, manager_id, hostname):
        self.manager_id = manager_id
        self.tstamp = time.time()
        self.hostname = hostname

    def __str__(self):
        return "Task failure due to loss of manager {} on host {}".format(self.manager_id.decode(), self.hostname)


class VersionMismatch(Exception):
    ''' Manager and Interchange versions do not match
    '''
    def __init__(self, interchange_version, manager_version):
        self.interchange_version = interchange_version
        self.manager_version = manager_version

    def __str__(self):
        return "Manager version info {} does not match interchange version info {}, causing a critical failure".format(
            self.manager_version,
            self.interchange_version)


class Interchange:
    """ Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    """
    def __init__(self,
                 client_address="127.0.0.1",
                 interchange_address: Optional[str] = None,
                 client_ports=(50055, 50056, 50057),
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 hub_address=None,
                 hub_port=None,
                 heartbeat_threshold=60,
                 logdir=".",
                 logging_level=logging.INFO,
                 poll_period=10,
             ) -> None:
        """
        Parameters
        ----------
        client_address : str
             The ip address at which the parsl client can be reached. Default: "127.0.0.1"

        interchange_address : Optional str
             If specified the interchange will only listen on this address for connections from workers
             else, it binds to all addresses.

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
             Logging level as defined in the logging module. Default: logging.INFO

        poll_period : int
             The main thread polling period, in milliseconds. Default: 10ms

        """
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        start_file_logger("{}/interchange.log".format(self.logdir), level=logging_level)
        logger.propagate = False
        logger.debug("Initializing Interchange process")

        self.client_address = client_address
        self.interchange_address: str = interchange_address or "*"
        self.poll_period = poll_period

        logger.info("Attempting connection to client at {} on ports: {},{},{}".format(
            client_address, client_ports[0], client_ports[1], client_ports[2]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))
        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))

        self.command_channel = self.context.socket(zmq.REP)
        self.command_channel.connect("tcp://{}:{}".format(client_address, client_ports[2]))
        logger.info("Connected to client")

        self.hub_address = hub_address
        self.hub_port = hub_port

        self.pending_task_queue: queue.Queue[Any] = queue.Queue(maxsize=10 ** 6)
        self.count = 0

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing.set_hwm(0)
        self.results_incoming = self.context.socket(zmq.ROUTER)
        self.results_incoming.set_hwm(0)

        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind(f"tcp://{self.interchange_address}:{self.worker_task_port}")
            self.results_incoming.bind(f"tcp://{self.interchange_address}:{self.worker_result_port}")

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port(f"tcp://{self.interchange_address}",
                                                                           min_port=worker_port_range[0],
                                                                           max_port=worker_port_range[1], max_tries=100)
            self.worker_result_port = self.results_incoming.bind_to_random_port(f"tcp://{self.interchange_address}",
                                                                                min_port=worker_port_range[0],
                                                                                max_port=worker_port_range[1], max_tries=100)

        logger.info("Bound to ports {},{} for incoming worker connections".format(
            self.worker_task_port, self.worker_result_port))

        self._ready_managers: Dict[bytes, ManagerRecord] = {}

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
                x = self.pending_task_queue.get(block=False)
            except queue.Empty:
                break
            else:
                tasks.append(x)

        return tasks

    @wrap_with_logs(target="interchange")
    def task_puller(self):
        """Pull tasks from the incoming tasks zmq pipe onto the internal
        pending task queue
        """
        logger.info("Starting")
        task_counter = 0

        while True:
            logger.debug("launching recv_pyobj")
            try:
                msg = self.task_incoming.recv_pyobj()
            except zmq.Again:
                # We just timed out while attempting to receive
                logger.debug("zmq.Again with {} tasks in internal queue".format(self.pending_task_queue.qsize()))
                continue

            logger.debug("putting message onto pending_task_queue")
            self.pending_task_queue.put(msg)
            task_counter += 1
            logger.debug(f"Fetched {task_counter} tasks so far")

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

    def _send_monitoring_info(self, hub_channel, manager: ManagerRecord):
        if hub_channel:
            logger.info("Sending message {} to hub".format(manager))

            d: Dict = cast(Dict, manager.copy())
            d['timestamp'] = datetime.datetime.now()
            d['last_heartbeat'] = datetime.datetime.fromtimestamp(d['last_heartbeat'])

            hub_channel.send_pyobj((MessageType.NODE_INFO, d))

    @wrap_with_logs(target="interchange")
    def _command_server(self):
        """ Command server to run async command to the interchange
        """
        logger.debug("Command Server Starting")

        # Need to create a new ZMQ socket for command server thread
        hub_channel = self._create_monitoring_channel()

        reply: Any  # the type of reply depends on the command_req received (aka this needs dependent types...)

        while True:
            try:
                command_req = self.command_channel.recv_pyobj()
                logger.debug("Received command request: {}".format(command_req))
                if command_req == "OUTSTANDING_C":
                    outstanding = self.pending_task_queue.qsize()
                    for manager in self._ready_managers.values():
                        outstanding += len(manager['tasks'])
                    reply = outstanding

                elif command_req == "WORKERS":
                    num_workers = 0
                    for manager in self._ready_managers.values():
                        num_workers += manager['worker_count']
                    reply = num_workers

                elif command_req == "MANAGERS":
                    reply = []
                    for manager_id in self._ready_managers:
                        m = self._ready_managers[manager_id]
                        idle_since = m['idle_since']
                        if idle_since is not None:
                            idle_duration = time.time() - idle_since
                        else:
                            idle_duration = 0.0
                        resp = {'manager': manager_id.decode('utf-8'),
                                'block_id': m['block_id'],
                                'worker_count': m['worker_count'],
                                'tasks': len(m['tasks']),
                                'idle_duration': idle_duration,
                                'active': m['active']}
                        reply.append(resp)

                elif command_req.startswith("HOLD_WORKER"):
                    cmd, s_manager = command_req.split(';')
                    manager_id = s_manager.encode('utf-8')
                    logger.info("Received HOLD_WORKER for {}".format(manager_id))
                    if manager_id in self._ready_managers:
                        m = self._ready_managers[manager_id]
                        m['active'] = False
                        self._send_monitoring_info(hub_channel, m)
                    else:
                        logger.warning("Worker to hold was not in ready managers list")

                    reply = None

                else:
                    reply = None

                logger.debug("Reply: {}".format(reply))
                self.command_channel.send_pyobj(reply)

            except zmq.Again:
                logger.debug("Command thread is alive")
                continue

    @wrap_with_logs
    def start(self):
        """ Start the interchange
        """

        # If a user workflow has set its own signal handler for sigterm, that
        # handler will be inherited by the interchange process because it is
        # launched as a multiprocessing fork process.
        # That can interfere with the interchange shutdown mechanism, which is
        # to receive a SIGTERM and exit immediately.
        # See Parsl issue #2343 (Threads and multiprocessing cannot be
        # intermingled without deadlocks) which talks about other fork-related
        # parent-process-inheritance problems.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        logger.info("Incoming ports bound")

        hub_channel = self._create_monitoring_channel()

        poll_period = self.poll_period

        start = time.time()

        self._task_puller_thread = threading.Thread(target=self.task_puller,
                                                    name="Interchange-Task-Puller",
                                                    daemon=True)
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(target=self._command_server,
                                                name="Interchange-Command",
                                                daemon=True)
        self._command_thread.start()

        kill_event = threading.Event()

        poller = zmq.Poller()
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        # These are managers which we should examine in an iteration
        # for scheduling a job (or maybe any other attention?).
        # Anything altering the state of the manager should add it
        # onto this list.
        interesting_managers: Set[bytes] = set()

        while not kill_event.is_set():
            self.socks = dict(poller.poll(timeout=poll_period))

            self.process_task_outgoing_incoming(interesting_managers, hub_channel, kill_event)
            self.process_results_incoming(interesting_managers, hub_channel)
            self.expire_bad_managers(interesting_managers, hub_channel)
            self.process_tasks_to_send(interesting_managers)

        delta = time.time() - start
        logger.info("Processed {} tasks in {} seconds".format(self.count, delta))
        logger.warning("Exiting")

    def process_task_outgoing_incoming(self, interesting_managers, hub_channel, kill_event):
        # Listen for requests for work
        if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
            logger.debug("starting task_outgoing section")
            message = self.task_outgoing.recv_multipart()
            manager_id = message[0]

            if manager_id not in self._ready_managers:
                reg_flag = False

                try:
                    msg = json.loads(message[1].decode('utf-8'))
                    reg_flag = True
                except Exception:
                    logger.warning("Got Exception reading registration message from manager: {}".format(
                        manager_id), exc_info=True)
                    logger.debug("Message: \n{}\n".format(message[1]))
                else:
                    # We set up an entry only if registration works correctly
                    self._ready_managers[manager_id] = {'last_heartbeat': time.time(),
                                                        'idle_since': time.time(),
                                                        'block_id': None,
                                                        'max_capacity': 0,
                                                        'worker_count': 0,
                                                        'active': True,
                                                        'tasks': []}
                if reg_flag is True:
                    interesting_managers.add(manager_id)
                    logger.info("Adding manager: {} to ready queue".format(manager_id))
                    m = self._ready_managers[manager_id]
                    m.update(msg)
                    logger.info("Registration info for manager {}: {}".format(manager_id, msg))
                    self._send_monitoring_info(hub_channel, m)

                    if (msg['python_v'].rsplit(".", 1)[0] != self.current_platform['python_v'].rsplit(".", 1)[0] or
                        msg['parsl_v'] != self.current_platform['parsl_v']):
                        logger.error("Manager {} has incompatible version info with the interchange".format(manager_id))
                        logger.debug("Setting kill event")
                        kill_event.set()
                        e = VersionMismatch("py.v={} parsl.v={}".format(self.current_platform['python_v'].rsplit(".", 1)[0],
                                                                        self.current_platform['parsl_v']),
                                            "py.v={} parsl.v={}".format(msg['python_v'].rsplit(".", 1)[0],
                                                                        msg['parsl_v'])
                        )
                        result_package = {'type': 'result', 'task_id': -1, 'exception': serialize_object(e)}
                        pkl_package = pickle.dumps(result_package)
                        self.results_outgoing.send(pkl_package)
                        logger.error("Sent failure reports, shutting down interchange")
                    else:
                        logger.info("Manager {} has compatible Parsl version {}".format(manager_id, msg['parsl_v']))
                        logger.info("Manager {} has compatible Python version {}".format(manager_id,
                                                                                         msg['python_v'].rsplit(".", 1)[0]))
                else:
                    # Registration has failed.
                    logger.debug("Suppressing bad registration from manager: {}".format(
                        manager_id))

            else:
                tasks_requested = int.from_bytes(message[1], "little")
                self._ready_managers[manager_id]['last_heartbeat'] = time.time()
                if tasks_requested == HEARTBEAT_CODE:
                    logger.debug("Manager {} sent heartbeat via tasks connection".format(manager_id))
                    self.task_outgoing.send_multipart([manager_id, b'', PKL_HEARTBEAT_CODE])
                else:
                    logger.error("Unexpected non-heartbeat message received from manager {}")
            logger.debug("leaving task_outgoing section")

    def process_tasks_to_send(self, interesting_managers):
        # If we had received any requests, check if there are tasks that could be passed

        logger.debug("Managers count (interesting/total): {interesting}/{total}".format(
            total=len(self._ready_managers),
            interesting=len(interesting_managers)))

        if interesting_managers and not self.pending_task_queue.empty():
            shuffled_managers = list(interesting_managers)
            random.shuffle(shuffled_managers)

            while shuffled_managers and not self.pending_task_queue.empty():  # cf. the if statement above...
                manager_id = shuffled_managers.pop()
                m = self._ready_managers[manager_id]
                tasks_inflight = len(m['tasks'])
                real_capacity = m['max_capacity'] - tasks_inflight

                if (real_capacity and m['active']):
                    tasks = self.get_tasks(real_capacity)
                    if tasks:
                        self.task_outgoing.send_multipart([manager_id, b'', pickle.dumps(tasks)])
                        task_count = len(tasks)
                        self.count += task_count
                        tids = [t['task_id'] for t in tasks]
                        m['tasks'].extend(tids)
                        m['idle_since'] = None
                        logger.debug("Sent tasks: {} to manager {}".format(tids, manager_id))
                        # recompute real_capacity after sending tasks
                        real_capacity = m['max_capacity'] - tasks_inflight
                        if real_capacity > 0:
                            logger.debug("Manager {} has free capacity {}".format(manager_id, real_capacity))
                            # ... so keep it in the interesting_managers list
                        else:
                            logger.debug("Manager {} is now saturated".format(manager_id))
                            interesting_managers.remove(manager_id)
                else:
                    interesting_managers.remove(manager_id)
                    # logger.debug("Nothing to send to manager {}".format(manager_id))
            logger.debug("leaving _ready_managers section, with {} managers still interesting".format(len(interesting_managers)))
        else:
            logger.debug("either no interesting managers or no tasks, so skipping manager pass")

    def process_results_incoming(self, interesting_managers, hub_channel):
        # Receive any results and forward to client
        if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
            logger.debug("entering results_incoming section")
            manager_id, *all_messages = self.results_incoming.recv_multipart()
            if manager_id not in self._ready_managers:
                logger.warning("Received a result from a un-registered manager: {}".format(manager_id))
            else:
                logger.debug(f"Got {len(all_messages)} result items in batch from manager {manager_id}")

                b_messages = []

                for p_message in all_messages:
                    r = pickle.loads(p_message)
                    if r['type'] == 'result':
                        # process this for task ID and forward to executor
                        b_messages.append((p_message, r))
                    elif r['type'] == 'monitoring':
                        hub_channel.send_pyobj(r['payload'])
                    elif r['type'] == 'heartbeat':
                        logger.debug(f"Manager {manager_id} sent heartbeat via results connection")
                        b_messages.append((p_message, r))
                    else:
                        logger.error("Interchange discarding result_queue message of unknown type: {}".format(r['type']))

                got_result = False
                m = self._ready_managers[manager_id]
                for (b_message, r) in b_messages:
                    assert 'type' in r, f"Message is missing type entry: {r}"
                    if r['type'] == 'result':
                        got_result = True
                        try:
                            logger.debug(f"Removing task {r['task_id']} from manager record {manager_id}")
                            m['tasks'].remove(r['task_id'])
                        except Exception:
                            # If we reach here, there's something very wrong.
                            logger.exception("Ignoring exception removing task_id {} for manager {} with task list {}".format(
                                r['task_id'],
                                manager_id,
                                m['tasks']))

                b_messages_to_send = []
                for (b_message, _) in b_messages:
                    b_messages_to_send.append(b_message)

                if b_messages_to_send:
                    logger.debug("Sending messages on results_outgoing")
                    self.results_outgoing.send_multipart(b_messages_to_send)
                    logger.debug("Sent messages on results_outgoing")

                logger.debug(f"Current tasks on manager {manager_id}: {m['tasks']}")
                if len(m['tasks']) == 0 and m['idle_since'] is None:
                    m['idle_since'] = time.time()

                # A manager is only made interesting here if a result was
                # received, which means there should be capacity for a new
                # task now. Heartbeats and monitoring messages do not make a
                # manager become interesting.
                if got_result:
                    interesting_managers.add(manager_id)
            logger.debug("leaving results_incoming section")

    def expire_bad_managers(self, interesting_managers, hub_channel):
        bad_managers = [(manager_id, m) for (manager_id, m) in self._ready_managers.items() if
                        time.time() - m['last_heartbeat'] > self.heartbeat_threshold]
        for (manager_id, m) in bad_managers:
            logger.debug("Last: {} Current: {}".format(m['last_heartbeat'], time.time()))
            logger.warning(f"Too many heartbeats missed for manager {manager_id} - removing manager")
            if m['active']:
                m['active'] = False
                self._send_monitoring_info(hub_channel, m)

            logger.warning(f"Cancelling htex tasks {m['tasks']} on removed manager")
            for tid in m['tasks']:
                try:
                    raise ManagerLost(manager_id, m['hostname'])
                except Exception:
                    result_package = {'type': 'result', 'task_id': tid, 'exception': serialize_object(RemoteExceptionWrapper(*sys.exc_info()))}
                    pkl_package = pickle.dumps(result_package)
                    self.results_outgoing.send(pkl_package)
            logger.warning("Sent failure reports, unregistering manager")
            self._ready_managers.pop(manager_id, 'None')
            if manager_id in interesting_managers:
                interesting_managers.remove(manager_id)


def start_file_logger(filename, level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
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
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d %(processName)s(%(process)d) %(threadName)s %(funcName)s [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(LOGGER_NAME)
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
