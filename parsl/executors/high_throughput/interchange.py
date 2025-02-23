#!/usr/bin/env python
import datetime
import json
import logging
import os
import pickle
import platform
import queue
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast

import zmq

from parsl import curvezmq
from parsl.addresses import tcp_url
from parsl.app.errors import RemoteExceptionWrapper
from parsl.executors.high_throughput.errors import ManagerLost, VersionMismatch
from parsl.executors.high_throughput.manager_record import ManagerRecord
from parsl.executors.high_throughput.manager_selector import ManagerSelector
from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.base import MonitoringRadioSender
from parsl.monitoring.radios.zmq import ZMQRadioSender
from parsl.process_loggers import wrap_with_logs
from parsl.serialize import serialize as serialize_object
from parsl.utils import setproctitle
from parsl.version import VERSION as PARSL_VERSION

PKL_HEARTBEAT_CODE = pickle.dumps((2 ** 32) - 1)
PKL_DRAINED_CODE = pickle.dumps((2 ** 32) - 2)

LOGGER_NAME = "interchange"
logger = logging.getLogger(LOGGER_NAME)


class Interchange:
    """ Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    """
    def __init__(self,
                 *,
                 client_address: str,
                 interchange_address: Optional[str],
                 client_ports: Tuple[int, int, int],
                 worker_ports: Optional[Tuple[int, int]],
                 worker_port_range: Tuple[int, int],
                 hub_address: Optional[str],
                 hub_zmq_port: Optional[int],
                 heartbeat_threshold: int,
                 logdir: str,
                 logging_level: int,
                 poll_period: int,
                 cert_dir: Optional[str],
                 manager_selector: ManagerSelector,
                 run_id: str,
                 ) -> None:
        """
        Parameters
        ----------
        client_address : str
             The ip address at which the parsl client can be reached. Default: "127.0.0.1"

        interchange_address : Optional str
             If specified the interchange will only listen on this address for connections from workers
             else, it binds to all addresses.

        client_ports : tuple(int, int, int)
             The ports at which the client can be reached

        worker_ports : tuple(int, int)
             The specific two ports at which workers will connect to the Interchange.

        worker_port_range : tuple(int, int)
             The interchange picks ports at random from the range which will be used by workers.
             This is overridden when the worker_ports option is set.

        hub_address : str
             The IP address at which the interchange can send info about managers to when monitoring is enabled.
             When None, monitoring is disabled.

        hub_zmq_port : str
             The port at which the interchange can send info about managers to when monitoring is enabled.
             When None, monitoring is disabled.

        heartbeat_threshold : int
             Number of seconds since the last heartbeat after which worker is considered lost.

        logdir : str
             Parsl log directory paths. Logs and temp files go here.

        logging_level : int
             Logging level as defined in the logging module.

        poll_period : int
             The main thread polling period, in milliseconds.

        cert_dir : str | None
            Path to the certificate directory.
        """
        self.cert_dir = cert_dir
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        start_file_logger("{}/interchange.log".format(self.logdir), level=logging_level)
        logger.debug("Initializing Interchange process")

        self.client_address = client_address
        self.interchange_address: str = interchange_address or "*"
        self.poll_period = poll_period

        logger.info("Attempting connection to client at {} on ports: {},{},{}".format(
            client_address, client_ports[0], client_ports[1], client_ports[2]))
        self.zmq_context = curvezmq.ServerContext(self.cert_dir)
        self.task_incoming = self.zmq_context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)
        self.task_incoming.connect(tcp_url(client_address, client_ports[0]))
        self.results_outgoing = self.zmq_context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        self.results_outgoing.connect(tcp_url(client_address, client_ports[1]))

        self.command_channel = self.zmq_context.socket(zmq.REP)
        self.command_channel.connect(tcp_url(client_address, client_ports[2]))
        logger.info("Connected to client")

        self.run_id = run_id

        self.hub_address = hub_address
        self.hub_zmq_port = hub_zmq_port

        self.pending_task_queue: queue.Queue[Any] = queue.Queue(maxsize=10 ** 6)

        # count of tasks that have been received from the submit side
        self.task_counter = 0

        # count of tasks that have been sent out to worker pools
        self.count = 0

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        self.task_outgoing = self.zmq_context.socket(zmq.ROUTER)
        self.task_outgoing.set_hwm(0)
        self.results_incoming = self.zmq_context.socket(zmq.ROUTER)
        self.results_incoming.set_hwm(0)

        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind(tcp_url(self.interchange_address, self.worker_task_port))
            self.results_incoming.bind(tcp_url(self.interchange_address, self.worker_result_port))

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port(tcp_url(self.interchange_address),
                                                                           min_port=worker_port_range[0],
                                                                           max_port=worker_port_range[1], max_tries=100)
            self.worker_result_port = self.results_incoming.bind_to_random_port(tcp_url(self.interchange_address),
                                                                                min_port=worker_port_range[0],
                                                                                max_port=worker_port_range[1], max_tries=100)

        logger.info("Bound to ports {},{} for incoming worker connections".format(
            self.worker_task_port, self.worker_result_port))

        self._ready_managers: Dict[bytes, ManagerRecord] = {}
        self.connected_block_history: List[str] = []

        self.heartbeat_threshold = heartbeat_threshold

        self.manager_selector = manager_selector

        self.current_platform = {'parsl_v': PARSL_VERSION,
                                 'python_v': "{}.{}.{}".format(sys.version_info.major,
                                                               sys.version_info.minor,
                                                               sys.version_info.micro),
                                 'os': platform.system(),
                                 'hostname': platform.node(),
                                 'dir': os.getcwd()}

        logger.info("Platform info: {}".format(self.current_platform))

    def get_tasks(self, count: int) -> Sequence[dict]:
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
        for _ in range(0, count):
            try:
                x = self.pending_task_queue.get(block=False)
            except queue.Empty:
                break
            else:
                tasks.append(x)

        return tasks

    def _send_monitoring_info(self, monitoring_radio: Optional[MonitoringRadioSender], manager: ManagerRecord) -> None:
        if monitoring_radio:
            logger.info("Sending message {} to MonitoringHub".format(manager))

            d: Dict = cast(Dict, manager.copy())
            d['timestamp'] = datetime.datetime.now()
            d['last_heartbeat'] = datetime.datetime.fromtimestamp(d['last_heartbeat'])
            d['run_id'] = self.run_id

            monitoring_radio.send((MessageType.NODE_INFO, d))

    def process_command(self, monitoring_radio: Optional[MonitoringRadioSender]) -> None:
        """ Command server to run async command to the interchange
        """
        logger.debug("entering command_server section")

        reply: Any  # the type of reply depends on the command_req received (aka this needs dependent types...)

        if self.command_channel in self.socks and self.socks[self.command_channel] == zmq.POLLIN:

            command_req = self.command_channel.recv_pyobj()
            logger.debug("Received command request: {}".format(command_req))
            if command_req == "CONNECTED_BLOCKS":
                reply = self.connected_block_history

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
                            'active': m['active'],
                            'parsl_version': m['parsl_version'],
                            'python_version': m['python_version'],
                            'draining': m['draining']}
                    reply.append(resp)

            elif command_req == "MANAGERS_PACKAGES":
                reply = {}
                for manager_id in self._ready_managers:
                    m = self._ready_managers[manager_id]
                    manager_id_str = manager_id.decode('utf-8')
                    reply[manager_id_str] = m["packages"]

            elif command_req.startswith("HOLD_WORKER"):
                cmd, s_manager = command_req.split(';')
                manager_id = s_manager.encode('utf-8')
                logger.info("Received HOLD_WORKER for {!r}".format(manager_id))
                if manager_id in self._ready_managers:
                    m = self._ready_managers[manager_id]
                    m['active'] = False
                    self._send_monitoring_info(monitoring_radio, m)
                else:
                    logger.warning("Worker to hold was not in ready managers list")

                reply = None

            elif command_req == "WORKER_PORTS":
                reply = (self.worker_task_port, self.worker_result_port)

            else:
                logger.error(f"Received unknown command: {command_req}")
                reply = None

            logger.debug("Reply: {}".format(reply))
            self.command_channel.send_pyobj(reply)

    @wrap_with_logs
    def start(self) -> None:
        """ Start the interchange
        """

        logger.info("Starting main interchange method")

        if self.hub_address is not None and self.hub_zmq_port is not None:
            logger.debug("Creating monitoring radio to %s:%s", self.hub_address, self.hub_zmq_port)
            monitoring_radio = ZMQRadioSender(self.hub_address, self.hub_zmq_port)
            logger.debug("Created monitoring radio")
        else:
            monitoring_radio = None

        poll_period = self.poll_period

        start = time.time()

        kill_event = threading.Event()

        poller = zmq.Poller()
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)
        poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.command_channel, zmq.POLLIN)

        # These are managers which we should examine in an iteration
        # for scheduling a job (or maybe any other attention?).
        # Anything altering the state of the manager should add it
        # onto this list.
        interesting_managers: Set[bytes] = set()

        while not kill_event.is_set():
            self.socks = dict(poller.poll(timeout=poll_period))

            self.process_command(monitoring_radio)
            self.process_task_incoming()
            self.process_task_outgoing_incoming(interesting_managers, monitoring_radio, kill_event)
            self.process_results_incoming(interesting_managers, monitoring_radio)
            self.expire_bad_managers(interesting_managers, monitoring_radio)
            self.expire_drained_managers(interesting_managers, monitoring_radio)
            self.process_tasks_to_send(interesting_managers)

        self.zmq_context.destroy()
        delta = time.time() - start
        logger.info(f"Processed {self.count} tasks in {delta} seconds")
        logger.warning("Exiting")

    def process_task_incoming(self) -> None:
        """Process incoming task message(s).
        """

        if self.task_incoming in self.socks and self.socks[self.task_incoming] == zmq.POLLIN:
            logger.debug("start task_incoming section")
            msg = self.task_incoming.recv_pyobj()
            logger.debug("putting message onto pending_task_queue")
            self.pending_task_queue.put(msg)
            self.task_counter += 1
            logger.debug(f"Fetched {self.task_counter} tasks so far")

    def process_task_outgoing_incoming(
            self,
            interesting_managers: Set[bytes],
            monitoring_radio: Optional[MonitoringRadioSender],
            kill_event: threading.Event
    ) -> None:
        """Process one message from manager on the task_outgoing channel.
        Note that this message flow is in contradiction to the name of the
        channel - it is not an outgoing message and it is not a task.
        """
        if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
            logger.debug("starting task_outgoing section")
            message = self.task_outgoing.recv_multipart()
            manager_id = message[0]

            try:
                msg = json.loads(message[1].decode('utf-8'))
            except Exception:
                logger.warning(f"Got Exception reading message from manager: {manager_id!r}", exc_info=True)
                logger.debug("Message:\n %r\n", message[1])
                return

            # perform a bit of validation on the structure of the deserialized
            # object, at least enough to behave like a deserialization error
            # in obviously malformed cases
            if not isinstance(msg, dict) or 'type' not in msg:
                logger.error(f"JSON message was not correctly formatted from manager: {manager_id!r}")
                logger.debug("Message:\n %r\n", message[1])
                return

            if msg['type'] == 'registration':
                # We set up an entry only if registration works correctly
                self._ready_managers[manager_id] = {'last_heartbeat': time.time(),
                                                    'idle_since': time.time(),
                                                    'block_id': None,
                                                    'start_time': msg['start_time'],
                                                    'max_capacity': 0,
                                                    'worker_count': 0,
                                                    'active': True,
                                                    'draining': False,
                                                    'parsl_version': msg['parsl_v'],
                                                    'python_version': msg['python_v'],
                                                    'tasks': []}
                self.connected_block_history.append(msg['block_id'])

                interesting_managers.add(manager_id)
                logger.info(f"Adding manager: {manager_id!r} to ready queue")
                m = self._ready_managers[manager_id]

                # m is a ManagerRecord, but msg is a dict[Any,Any] and so can
                # contain arbitrary fields beyond those in ManagerRecord (and
                # indeed does - for example, python_v) which are then ignored
                # later.
                m.update(msg)  # type: ignore[typeddict-item]

                logger.info(f"Registration info for manager {manager_id!r}: {msg}")
                self._send_monitoring_info(monitoring_radio, m)

                if (msg['python_v'].rsplit(".", 1)[0] != self.current_platform['python_v'].rsplit(".", 1)[0] or
                    msg['parsl_v'] != self.current_platform['parsl_v']):
                    logger.error(f"Manager {manager_id!r} has incompatible version info with the interchange")
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
                    logger.info(f"Manager {manager_id!r} has compatible Parsl version {msg['parsl_v']}")
                    logger.info(f"Manager {manager_id!r} has compatible Python version {msg['python_v'].rsplit('.', 1)[0]}")
            elif msg['type'] == 'heartbeat':
                manager = self._ready_managers.get(manager_id)
                if manager:
                    manager['last_heartbeat'] = time.time()
                    logger.debug("Manager %r sent heartbeat via tasks connection", manager_id)
                    self.task_outgoing.send_multipart([manager_id, b'', PKL_HEARTBEAT_CODE])
                else:
                    logger.warning("Received heartbeat via tasks connection for not-registered manager %r", manager_id)
            elif msg['type'] == 'drain':
                self._ready_managers[manager_id]['draining'] = True
                logger.debug("Manager %r requested drain", manager_id)
            else:
                logger.error(f"Unexpected message type received from manager: {msg['type']}")
            logger.debug("leaving task_outgoing section")

    def expire_drained_managers(self, interesting_managers: Set[bytes], monitoring_radio: Optional[MonitoringRadioSender]) -> None:

        for manager_id in list(interesting_managers):
            # is it always true that a draining manager will be in interesting managers?
            # i think so because it will have outstanding capacity?
            m = self._ready_managers[manager_id]
            if m['draining'] and len(m['tasks']) == 0:
                logger.info(f"Manager {manager_id!r} is drained - sending drained message to manager")
                self.task_outgoing.send_multipart([manager_id, b'', PKL_DRAINED_CODE])
                interesting_managers.remove(manager_id)
                self._ready_managers.pop(manager_id)

                m['active'] = False
                self._send_monitoring_info(monitoring_radio, m)

    def process_tasks_to_send(self, interesting_managers: Set[bytes]) -> None:
        # Check if there are tasks that could be sent to managers

        logger.debug(
            "Managers count (interesting/total): %d/%d",
            len(interesting_managers),
            len(self._ready_managers)
        )

        if interesting_managers and not self.pending_task_queue.empty():
            shuffled_managers = self.manager_selector.sort_managers(self._ready_managers, interesting_managers)

            while shuffled_managers and not self.pending_task_queue.empty():  # cf. the if statement above...
                manager_id = shuffled_managers.pop()
                m = self._ready_managers[manager_id]
                tasks_inflight = len(m['tasks'])
                real_capacity = m['max_capacity'] - tasks_inflight

                if real_capacity and m["active"] and not m["draining"]:
                    tasks = self.get_tasks(real_capacity)
                    if tasks:
                        self.task_outgoing.send_multipart([manager_id, b'', pickle.dumps(tasks)])
                        task_count = len(tasks)
                        self.count += task_count
                        tids = [t['task_id'] for t in tasks]
                        m['tasks'].extend(tids)
                        m['idle_since'] = None
                        logger.debug("Sent tasks: %s to manager %r", tids, manager_id)
                        # recompute real_capacity after sending tasks
                        real_capacity = m['max_capacity'] - tasks_inflight
                        if real_capacity > 0:
                            logger.debug("Manager %r has free capacity %s", manager_id, real_capacity)
                            # ... so keep it in the interesting_managers list
                        else:
                            logger.debug("Manager %r is now saturated", manager_id)
                            interesting_managers.remove(manager_id)
                else:
                    interesting_managers.remove(manager_id)
                    # logger.debug("Nothing to send to manager {}".format(manager_id))
            logger.debug("leaving _ready_managers section, with %s managers still interesting", len(interesting_managers))
        else:
            logger.debug("either no interesting managers or no tasks, so skipping manager pass")

    def process_results_incoming(self, interesting_managers: Set[bytes], monitoring_radio: Optional[MonitoringRadioSender]) -> None:
        # Receive any results and forward to client
        if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
            logger.debug("entering results_incoming section")
            manager_id, *all_messages = self.results_incoming.recv_multipart()
            if manager_id not in self._ready_managers:
                logger.warning(f"Received a result from a un-registered manager: {manager_id!r}")
            else:
                logger.debug("Got %s result items in batch from manager %r", len(all_messages), manager_id)

                b_messages = []

                for p_message in all_messages:
                    r = pickle.loads(p_message)
                    if r['type'] == 'result':
                        # process this for task ID and forward to executor
                        b_messages.append((p_message, r))
                    elif r['type'] == 'monitoring':
                        # the monitoring code makes the assumption that no
                        # monitoring messages will be received if monitoring
                        # is not configured, and that monitoring_radio will only
                        # be None when monitoring is not configurated.
                        assert monitoring_radio is not None

                        monitoring_radio.send(r['payload'])
                    elif r['type'] == 'heartbeat':
                        logger.debug("Manager %r sent heartbeat via results connection", manager_id)
                    else:
                        logger.error("Interchange discarding result_queue message of unknown type: %s", r["type"])

                got_result = False
                m = self._ready_managers[manager_id]
                for (_, r) in b_messages:
                    assert 'type' in r, f"Message is missing type entry: {r}"
                    if r['type'] == 'result':
                        got_result = True
                        try:
                            logger.debug("Removing task %s from manager record %r", r["task_id"], manager_id)
                            m['tasks'].remove(r['task_id'])
                        except Exception:
                            # If we reach here, there's something very wrong.
                            logger.exception(
                                "Ignoring exception removing task_id %s for manager %r with task list %s",
                                r['task_id'],
                                manager_id,
                                m["tasks"]
                            )

                b_messages_to_send = []
                for (b_message, _) in b_messages:
                    b_messages_to_send.append(b_message)

                if b_messages_to_send:
                    logger.debug("Sending messages on results_outgoing")
                    self.results_outgoing.send_multipart(b_messages_to_send)
                    logger.debug("Sent messages on results_outgoing")

                logger.debug("Current tasks on manager %r: %s", manager_id, m["tasks"])
                if len(m['tasks']) == 0 and m['idle_since'] is None:
                    m['idle_since'] = time.time()

                # A manager is only made interesting here if a result was
                # received, which means there should be capacity for a new
                # task now. Heartbeats and monitoring messages do not make a
                # manager become interesting.
                if got_result:
                    interesting_managers.add(manager_id)
            logger.debug("leaving results_incoming section")

    def expire_bad_managers(self, interesting_managers: Set[bytes], monitoring_radio: Optional[MonitoringRadioSender]) -> None:
        bad_managers = [(manager_id, m) for (manager_id, m) in self._ready_managers.items() if
                        time.time() - m['last_heartbeat'] > self.heartbeat_threshold]
        for (manager_id, m) in bad_managers:
            logger.debug("Last: {} Current: {}".format(m['last_heartbeat'], time.time()))
            logger.warning(f"Too many heartbeats missed for manager {manager_id!r} - removing manager")
            if m['active']:
                m['active'] = False
                self._send_monitoring_info(monitoring_radio, m)

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


def start_file_logger(filename: str, level: int = logging.DEBUG, format_string: Optional[str] = None) -> None:
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
        format_string = (

            "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d "
            "%(processName)s(%(process)d) %(threadName)s "
            "%(funcName)s [%(levelname)s] %(message)s"

        )

    global logger
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    setproctitle("parsl: HTEX interchange")

    config = pickle.load(sys.stdin.buffer)

    ic = Interchange(**config)
    ic.start()
