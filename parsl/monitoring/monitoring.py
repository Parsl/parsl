import os
import socket
import time
import pickle
import logging
import typeguard
import zmq

import queue

import parsl.monitoring.remote

from parsl.multiprocessing import ForkProcess, SizedQueue
from multiprocessing import Process, Queue
from parsl.utils import RepresentationMixin
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.serialize import deserialize

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.types import AddressedMonitoringMessage, TaggedMonitoringMessage
from typing import cast, Any, Callable, Dict, Optional, Sequence, Tuple, Union

_db_manager_excepts: Optional[Exception]


try:
    from parsl.monitoring.db_manager import dbm_starter
except Exception as e:
    _db_manager_excepts = e
else:
    _db_manager_excepts = None

logger = logging.getLogger(__name__)


def start_file_logger(filename: str, name: str = 'monitoring', level: int = logging.DEBUG, format_string: Optional[str] = None) -> logging.Logger:
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name.
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

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


@typeguard.typechecked
class MonitoringHub(RepresentationMixin):
    def __init__(self,
                 hub_address: str,
                 hub_port: Optional[int] = None,
                 hub_port_range: Tuple[int, int] = (55050, 56000),

                 client_address: str = "127.0.0.1",
                 client_port_range: Tuple[int, int] = (55000, 56000),

                 workflow_name: Optional[str] = None,
                 workflow_version: Optional[str] = None,
                 logging_endpoint: str = 'sqlite:///runinfo/monitoring.db',
                 logdir: Optional[str] = None,
                 monitoring_debug: bool = False,
                 resource_monitoring_enabled: bool = True,
                 resource_monitoring_interval: float = 30):  # in seconds
        """
        Parameters
        ----------
        hub_address : str
             The ip address at which the workers will be able to reach the Hub.
        hub_port : int
             The UDP port to which workers will be able to deliver messages to
             the monitoring router.
             Note that despite the similar name, this is not related to
             hub_port_range.
             Default: None
        hub_port_range : tuple(int, int)
             The port range for a ZMQ channel from an executor process
             (for example, the interchange in the High Throughput Executor)
             to deliver monitoring messages to the monitoring router.
             Note that despite the similar name, this is not related to hub_port.
             Default: (55050, 56000)
        client_address : str
             The ip address at which the dfk will be able to reach Hub. Default: "127.0.0.1"
        client_port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             Default: (55000, 56000)
        workflow_name : str
             The name for the workflow. Default to the name of the parsl script
        workflow_version : str
             The version of the workflow. Default to the beginning datetime of the parsl script
        logging_endpoint : str
             The database connection url for monitoring to log the information.
             These URLs follow RFC-1738, and can include username, password, hostname, database name.
             Default: 'sqlite:///monitoring.db'
        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        monitoring_debug : Bool
             Enable monitoring debug logging. Default: False
        resource_monitoring_enabled : boolean
             Set this field to True to enable logging of information from the worker side.
             This will include environment information such as start time, hostname and block id,
             along with periodic resource usage of each task. Default: True
        resource_monitoring_interval : float
             The time interval, in seconds, at which the monitoring records the resource usage of each task. Default: 30 seconds
        """

        self.logger = logger

        # Any is used to disable typechecking on uses of _dfk_channel,
        # because it is used in the code as if it points to a channel, but
        # the static type is that it can also be None. The code relies on
        # .start() being called and initialising this to a real channel.
        self._dfk_channel = None  # type: Any

        if _db_manager_excepts:
            raise _db_manager_excepts

        self.client_address = client_address
        self.client_port_range = client_port_range

        self.hub_address = hub_address
        self.hub_port = hub_port
        self.hub_port_range = hub_port_range

        self.logging_endpoint = logging_endpoint
        self.logdir = logdir
        self.monitoring_debug = monitoring_debug

        self.workflow_name = workflow_name
        self.workflow_version = workflow_version

        self.resource_monitoring_enabled = resource_monitoring_enabled
        self.resource_monitoring_interval = resource_monitoring_interval

    def start(self, run_id: str, run_dir: str) -> int:

        if self.logdir is None:
            self.logdir = "."

        os.makedirs(self.logdir, exist_ok=True)

        # Initialize the ZMQ pipe to the Parsl Client

        self.logger.debug("Initializing ZMQ Pipes to client")
        self.monitoring_hub_active = True

        comm_q: Queue[Union[Tuple[int, int], str]]
        comm_q = SizedQueue(maxsize=10)

        self.exception_q: Queue[Tuple[str, str]]
        self.exception_q = SizedQueue(maxsize=10)

        self.priority_msgs: Queue[Tuple[Any, int]]
        self.priority_msgs = SizedQueue()

        self.resource_msgs: Queue[AddressedMonitoringMessage]
        self.resource_msgs = SizedQueue()

        self.node_msgs: Queue[AddressedMonitoringMessage]
        self.node_msgs = SizedQueue()

        self.block_msgs: Queue[AddressedMonitoringMessage]
        self.block_msgs = SizedQueue()

        self.router_proc = ForkProcess(target=router_starter,
                                       args=(comm_q, self.exception_q, self.priority_msgs, self.node_msgs, self.block_msgs, self.resource_msgs),
                                       kwargs={"hub_address": self.hub_address,
                                               "hub_port": self.hub_port,
                                               "hub_port_range": self.hub_port_range,
                                               "logdir": self.logdir,
                                               "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                               "run_id": run_id
                                       },
                                       name="Monitoring-Router-Process",
                                       daemon=True,
        )
        self.router_proc.start()

        self.dbm_proc = ForkProcess(target=dbm_starter,
                                    args=(self.exception_q, self.priority_msgs, self.node_msgs, self.block_msgs, self.resource_msgs,),
                                    kwargs={"logdir": self.logdir,
                                            "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                            "db_url": self.logging_endpoint,
                                    },
                                    name="Monitoring-DBM-Process",
                                    daemon=True,
        )
        self.dbm_proc.start()
        self.logger.info("Started the router process {} and DBM process {}".format(self.router_proc.pid, self.dbm_proc.pid))

        self.filesystem_proc = Process(target=filesystem_receiver,
                                       args=(self.logdir, self.resource_msgs, run_dir),
                                       name="Monitoring-Filesystem-Process",
                                       daemon=True
        )
        self.filesystem_proc.start()
        self.logger.info(f"Started filesystem radio receiver process {self.filesystem_proc.pid}")

        try:
            comm_q_result = comm_q.get(block=True, timeout=120)
        except queue.Empty:
            self.logger.error("Hub has not completed initialization in 120s. Aborting")
            raise Exception("Hub failed to start")

        if isinstance(comm_q_result, str):
            self.logger.error(f"MonitoringRouter sent an error message: {comm_q_result}")
            raise RuntimeError(f"MonitoringRouter failed to start: {comm_q_result}")

        udp_port, ic_port = comm_q_result

        self.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_port)

        context = zmq.Context()
        self.dfk_channel_timeout = 10000  # in milliseconds
        self._dfk_channel = context.socket(zmq.DEALER)
        self._dfk_channel.setsockopt(zmq.LINGER, 0)
        self._dfk_channel.set_hwm(0)
        self._dfk_channel.setsockopt(zmq.SNDTIMEO, self.dfk_channel_timeout)
        self._dfk_channel.connect("tcp://{}:{}".format(self.hub_address, ic_port))

        self.logger.info("Monitoring Hub initialized")

        return ic_port

    # TODO: tighten the Any message format
    def send(self, mtype: MessageType, message: Any) -> None:
        self.logger.debug("Sending message type {}".format(mtype))
        try:
            self._dfk_channel.send_pyobj((mtype, message))
        except zmq.Again:
            self.logger.exception(
                "The monitoring message sent from DFK to router timed-out after {}ms".format(self.dfk_channel_timeout))

    def close(self) -> None:
        self.logger.info("Terminating Monitoring Hub")
        exception_msgs = []
        while True:
            try:
                exception_msgs.append(self.exception_q.get(block=False))
                self.logger.error("There was a queued exception (Either router or DBM process got exception much earlier?)")
            except queue.Empty:
                break
        if self._dfk_channel and self.monitoring_hub_active:
            self.monitoring_hub_active = False
            self._dfk_channel.close()
            if exception_msgs:
                for exception_msg in exception_msgs:
                    self.logger.error("{} process delivered an exception: {}. Terminating all monitoring processes immediately.".format(exception_msg[0],
                                      exception_msg[1]))
                self.router_proc.terminate()
                self.dbm_proc.terminate()
                self.filesystem_proc.terminate()
            self.logger.info("Waiting for router to terminate")
            self.router_proc.join()
            self.logger.debug("Finished waiting for router termination")
            if len(exception_msgs) == 0:
                self.logger.debug("Sending STOP to DBM")
                self.priority_msgs.put(("STOP", 0))
            else:
                self.logger.debug("Not sending STOP to DBM, because there were DBM exceptions")
            self.logger.debug("Waiting for DB termination")
            self.dbm_proc.join()
            self.logger.debug("Finished waiting for DBM termination")

            # should this be message based? it probably doesn't need to be if
            # we believe we've received all messages
            self.logger.info("Terminating filesystem radio receiver process")
            self.filesystem_proc.terminate()
            self.filesystem_proc.join()

    @staticmethod
    def monitor_wrapper(f: Any,
                        args: Sequence,
                        kwargs: Dict,
                        try_id: int,
                        task_id: int,
                        monitoring_hub_url: str,
                        run_id: str,
                        logging_level: int,
                        sleep_dur: float,
                        radio_mode: str,
                        monitor_resources: bool,
                        run_dir: str) -> Tuple[Callable, Sequence, Dict]:
        return parsl.monitoring.remote.monitor_wrapper(f, args, kwargs, try_id, task_id, monitoring_hub_url,
                                                       run_id, logging_level, sleep_dur, radio_mode,
                                                       monitor_resources, run_dir)


@wrap_with_logs
def filesystem_receiver(logdir: str, q: "queue.Queue[AddressedMonitoringMessage]", run_dir: str) -> None:
    logger = start_file_logger("{}/monitoring_filesystem_radio.log".format(logdir),
                               name="monitoring_filesystem_radio",
                               level=logging.INFO)

    logger.info("Starting filesystem radio receiver")
    setproctitle("parsl: monitoring filesystem receiver")
    base_path = f"{run_dir}/monitor-fs-radio/"
    tmp_dir = f"{base_path}/tmp/"
    new_dir = f"{base_path}/new/"
    logger.debug(f"Creating new and tmp paths under {base_path}")

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(new_dir, exist_ok=True)

    while True:  # this loop will end on process termination
        logger.debug("Start filesystem radio receiver loop")

        # iterate over files in new_dir
        for filename in os.listdir(new_dir):
            try:
                logger.info(f"Processing filesystem radio file {filename}")
                full_path_filename = f"{new_dir}/{filename}"
                with open(full_path_filename, "rb") as f:
                    message = deserialize(f.read())
                logger.debug(f"Message received is: {message}")
                assert isinstance(message, tuple)
                q.put(cast(AddressedMonitoringMessage, message))
                os.remove(full_path_filename)
            except Exception:
                logger.exception(f"Exception processing {filename} - probably will be retried next iteration")

        time.sleep(1)  # whats a good time for this poll?


class MonitoringRouter:

    def __init__(self,
                 *,
                 hub_address: str,
                 hub_port: Optional[int] = None,
                 hub_port_range: Tuple[int, int] = (55050, 56000),

                 monitoring_hub_address: str = "127.0.0.1",
                 logdir: str = ".",
                 run_id: str,
                 logging_level: int = logging.INFO,
                 atexit_timeout: int = 3    # in seconds
                ):
        """ Initializes a monitoring configuration class.

        Parameters
        ----------
        hub_address : str
             The ip address at which the workers will be able to reach the Hub.
        hub_port : int
             The specific port at which workers will be able to reach the Hub via UDP. Default: None
        hub_port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             This is overridden when the hub_port option is set. Default: (55050, 56000)
        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO
        atexit_timeout : float, optional
            The amount of time in seconds to terminate the hub without receiving any messages, after the last dfk workflow message is received.

        """
        os.makedirs(logdir, exist_ok=True)
        self.logger = start_file_logger("{}/monitoring_router.log".format(logdir),
                                        name="monitoring_router",
                                        level=logging_level)
        self.logger.debug("Monitoring router starting")

        self.hub_address = hub_address
        self.atexit_timeout = atexit_timeout
        self.run_id = run_id

        self.loop_freq = 10.0  # milliseconds

        # Initialize the UDP socket
        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)

        # We are trying to bind to all interfaces with 0.0.0.0
        if not hub_port:
            self.sock.bind(('0.0.0.0', 0))
            self.hub_port = self.sock.getsockname()[1]
        else:
            self.hub_port = hub_port
            try:
                self.sock.bind(('0.0.0.0', self.hub_port))
            except Exception as e:
                raise RuntimeError(f"Could not bind to hub_port {hub_port} because: {e}")
        self.sock.settimeout(self.loop_freq / 1000)
        self.logger.info("Initialized the UDP socket on 0.0.0.0:{}".format(self.hub_port))

        self._context = zmq.Context()
        self.ic_channel = self._context.socket(zmq.DEALER)
        self.ic_channel.setsockopt(zmq.LINGER, 0)
        self.ic_channel.set_hwm(0)
        self.ic_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.logger.debug("hub_address: {}. hub_port_range {}".format(hub_address, hub_port_range))
        self.ic_port = self.ic_channel.bind_to_random_port("tcp://*",
                                                           min_port=hub_port_range[0],
                                                           max_port=hub_port_range[1])

    def start(self,
              priority_msgs: "queue.Queue[AddressedMonitoringMessage]",
              node_msgs: "queue.Queue[AddressedMonitoringMessage]",
              block_msgs: "queue.Queue[AddressedMonitoringMessage]",
              resource_msgs: "queue.Queue[AddressedMonitoringMessage]") -> None:
        try:
            router_keep_going = True
            while router_keep_going:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    resource_msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, resource_msg))
                    resource_msgs.put((resource_msg, addr))
                except socket.timeout:
                    pass

                try:
                    dfk_loop_start = time.time()
                    while time.time() - dfk_loop_start < 1.0:  # TODO make configurable
                        # note that nothing checks that msg really is of the annotated type
                        msg: TaggedMonitoringMessage
                        msg = self.ic_channel.recv_pyobj()

                        assert isinstance(msg, tuple), "IC Channel expects only tuples, got {}".format(msg)
                        assert len(msg) >= 1, "IC Channel expects tuples of length at least 1, got {}".format(msg)
                        assert len(msg) == 2, "IC Channel expects message tuples of exactly length 2, got {}".format(msg)

                        msg_0: AddressedMonitoringMessage
                        msg_0 = (msg, 0)

                        if msg[0] == MessageType.NODE_INFO:
                            msg[1]['run_id'] = self.run_id
                            node_msgs.put(msg_0)
                        elif msg[0] == MessageType.RESOURCE_INFO:
                            resource_msgs.put(msg_0)
                        elif msg[0] == MessageType.BLOCK_INFO:
                            block_msgs.put(msg_0)
                        elif msg[0] == MessageType.TASK_INFO:
                            priority_msgs.put(msg_0)
                        elif msg[0] == MessageType.WORKFLOW_INFO:
                            priority_msgs.put(msg_0)
                            if 'exit_now' in msg[1] and msg[1]['exit_now']:
                                router_keep_going = False
                        else:
                            # There is a type: ignore here because if msg[0]
                            # is of the correct type, this code is unreachable,
                            # but there is no verification that the message
                            # received from ic_channel.recv_pyobj() is actually
                            # of that type.
                            self.logger.error(f"Discarding message from interchange with unknown type {msg[0].value}")  # type: ignore[unreachable]
                except zmq.Again:
                    pass
                except Exception:
                    # This will catch malformed messages. What happens if the
                    # channel is broken in such a way that it always raises
                    # an exception? Looping on this would maybe be the wrong
                    # thing to do.
                    self.logger.warning("Failure processing a ZMQ message", exc_info=True)

            self.logger.info("Monitoring router draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                    resource_msgs.put((msg, addr))
                    last_msg_received_time = time.time()
                except socket.timeout:
                    pass

            self.logger.info("Monitoring router finishing normally")
        finally:
            self.logger.info("Monitoring router finished")


@wrap_with_logs
def router_starter(comm_q: "queue.Queue[Union[Tuple[int, int], str]]",
                   exception_q: "queue.Queue[Tuple[str, str]]",
                   priority_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   node_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   block_msgs: "queue.Queue[AddressedMonitoringMessage]",
                   resource_msgs: "queue.Queue[AddressedMonitoringMessage]",

                   hub_address: str,
                   hub_port: Optional[int],
                   hub_port_range: Tuple[int, int],

                   logdir: str,
                   logging_level: int,
                   run_id: str) -> None:
    setproctitle("parsl: monitoring router")
    try:
        router = MonitoringRouter(hub_address=hub_address,
                                  hub_port=hub_port,
                                  hub_port_range=hub_port_range,
                                  logdir=logdir,
                                  logging_level=logging_level,
                                  run_id=run_id)
    except Exception as e:
        logger.error("MonitoringRouter construction failed.", exc_info=True)
        comm_q.put(f"Monitoring router construction failed: {e}")
    else:
        comm_q.put((router.hub_port, router.ic_port))

        router.logger.info("Starting MonitoringRouter in router_starter")
        try:
            router.start(priority_msgs, node_msgs, block_msgs, resource_msgs)
        except Exception as e:
            router.logger.exception("router.start exception")
            exception_q.put(('Hub', str(e)))
