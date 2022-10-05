import os
import socket
import time
import pickle
import logging
import typeguard
import datetime
import zmq
from functools import wraps

import queue
from parsl.multiprocessing import ForkProcess, SizedQueue
from multiprocessing import Event, Process, Queue
from parsl.utils import RepresentationMixin
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.serialize import deserialize

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios import MonitoringRadio, UDPRadio, HTEXRadio, FilesystemRadio
from parsl.monitoring.types import AddressedMonitoringMessage, TaggedMonitoringMessage
from typing import cast, Any, Callable, Dict, List, Optional, Union

_db_manager_excepts: Optional[Exception]

from typing import Optional, Tuple


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
            raise(_db_manager_excepts)

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

        comm_q = SizedQueue(maxsize=10)  # type: Queue[Union[Tuple[int, int], str]]
        self.exception_q = SizedQueue(maxsize=10)  # type: Queue[Tuple[str, str]]
        self.priority_msgs = SizedQueue()  # type: Queue[Tuple[Any, int]]
        self.resource_msgs = SizedQueue()  # type: Queue[AddressedMonitoringMessage]
        self.node_msgs = SizedQueue()  # type: Queue[AddressedMonitoringMessage]
        self.block_msgs = SizedQueue()  # type:  Queue[AddressedMonitoringMessage]

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
        if self.logger:
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
                        try_id: int,
                        task_id: int,
                        monitoring_hub_url: str,
                        run_id: str,
                        logging_level: int,
                        sleep_dur: float,
                        radio_mode: str,
                        monitor_resources: bool,
                        run_dir: str) -> Callable:
        """Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
        """
        @wraps(f)
        def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            terminate_event = Event()
            # Send first message to monitoring router
            send_first_message(try_id,
                               task_id,
                               monitoring_hub_url,
                               run_id,
                               radio_mode,
                               run_dir)

            p: Optional[Process]
            if monitor_resources:
                # create the monitor process and start
                pp = ForkProcess(target=monitor,
                                 args=(os.getpid(),
                                       try_id,
                                       task_id,
                                       monitoring_hub_url,
                                       run_id,
                                       radio_mode,
                                       logging_level,
                                       sleep_dur,
                                       run_dir,
                                       terminate_event),
                                 name="Monitor-Wrapper-{}".format(task_id))
                pp.start()
                p = pp
                #  TODO: awkwardness because ForkProcess is not directly a constructor
                # and type-checking is expecting p to be optional and cannot
                # narrow down the type of p in this block.

            else:
                p = None

            try:
                return f(*args, **kwargs)
            finally:
                # There's a chance of zombification if the workers are killed by some signals (?)
                if p:
                    terminate_event.set()
                    p.join(30)  # 30 second delay for this -- this timeout will be hit in the case of an unusually long end-of-loop
                    if p.exitcode is None:
                        logger.warn("Event-based termination of monitoring helper took too long. Using process-based termination.")
                        p.terminate()
                        # DANGER: this can corrupt shared queues according to docs.
                        # So, better that the above termination event worked.
                        # This is why this log message is a warning
                        p.join()

                send_last_message(try_id,
                                  task_id,
                                  monitoring_hub_url,
                                  run_id,
                                  radio_mode, run_dir)

        return wrapped


@wrap_with_logs
def filesystem_receiver(logdir: str, q: "queue.Queue[AddressedMonitoringMessage]", run_dir: str) -> None:
    logger = start_file_logger("{}/monitoring_filesystem_radio.log".format(logdir),
                               name="monitoring_filesystem_radio",
                               level=logging.DEBUG)

    logger.info("Starting filesystem radio receiver")
    setproctitle("parsl: monitoring filesystem receiver")
    base_path = f"{run_dir}/monitor-fs-radio/"
    tmp_dir = f"{base_path}/tmp/"
    new_dir = f"{base_path}/new/"
    logger.debug(f"Creating new and tmp paths under {base_path}")

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(new_dir, exist_ok=True)

    while True:  # this loop will end on process termination
        logger.info("Start filesystem radio receiver loop")

        # iterate over files in new_dir
        for filename in os.listdir(new_dir):
            try:
                logger.info(f"Processing filesystem radio file {filename}")
                full_path_filename = f"{new_dir}/{filename}"
                with open(full_path_filename, "rb") as f:
                    message = deserialize(f.read())
                logger.info(f"Message received is: {message}")
                assert(isinstance(message, tuple))
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
            self.sock.bind(('0.0.0.0', self.hub_port))
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
                            self.logger.error(f"Discarding message from interchange with unknown type {msg[0].value}")
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

    router.logger.info("End of router_starter")


@wrap_with_logs
def send_first_message(try_id: int,
                       task_id: int,
                       monitoring_hub_url: str,
                       run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, False)


@wrap_with_logs
def send_last_message(try_id: int,
                      task_id: int,
                      monitoring_hub_url: str,
                      run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, True)


def send_first_last_message(try_id: int,
                            task_id: int,
                            monitoring_hub_url: str,
                            run_id: str, radio_mode: str, run_dir: str,
                            is_last: bool) -> None:
    import platform
    import os

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': not is_last,
            'last_msg': is_last,
            'timestamp': datetime.datetime.now()
    })
    radio.send(msg)
    return


@wrap_with_logs
def monitor(pid: int,
            try_id: int,
            task_id: int,
            monitoring_hub_url: str,
            run_id: str,
            radio_mode: str,
            logging_level: int,
            sleep_dur: float,
            run_dir: str,
            # removed all defaults because unused and there's no meaningful default for terminate_event.
            # these probably should become named arguments, with a *, and named at invocation.
            terminate_event: Any) -> None:  # cannot be Event because of multiprocessing type weirdness.
    """Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.

    This process makes calls to logging, but deliberately does not attach
    any log handlers. Previously, there was a handler which logged to a
    file in /tmp, but this was usually not useful or even accessible.
    In some circumstances, it might be useful to hack in a handler so the
    logger calls remain in place.
    """
    import logging
    import platform
    import psutil

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    logging.debug("start of monitor")

    # these values are simple to log. Other information is available in special formats such as memory below.
    simple = ["cpu_num", 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    # values that can be summed up to see total resources used by task process and its children
    summable_values = ['memory_percent', 'num_threads']

    pm = psutil.Process(pid)

    children_user_time = {}  # type: Dict[int, float]
    children_system_time = {}  # type: Dict[int, float]

    def accumulate_and_prepare() -> Dict[str, Any]:
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["run_id"] = run_id
        d["task_id"] = task_id
        d["try_id"] = try_id
        d['resource_monitoring_interval'] = sleep_dur
        d['hostname'] = platform.node()
        d['first_msg'] = False
        d['last_msg'] = False
        d['timestamp'] = datetime.datetime.now()

        logging.debug("getting children")
        children = pm.children(recursive=True)
        logging.debug("got children")

        d["psutil_cpu_count"] = psutil.cpu_count()
        d['psutil_process_memory_virtual'] = pm.memory_info().vms
        d['psutil_process_memory_resident'] = pm.memory_info().rss
        d['psutil_process_time_user'] = pm.cpu_times().user
        d['psutil_process_time_system'] = pm.cpu_times().system
        d['psutil_process_children_count'] = len(children)
        try:
            d['psutil_process_disk_write'] = pm.io_counters().write_chars
            d['psutil_process_disk_read'] = pm.io_counters().read_chars
        except Exception:
            # occasionally pid temp files that hold this information are unvailable to be read so set to zero
            logging.exception("Exception reading IO counters for main process. Recorded IO usage may be incomplete", exc_info=True)
            d['psutil_process_disk_write'] = 0
            d['psutil_process_disk_read'] = 0
        for child in children:
            for k, v in child.as_dict(attrs=summable_values).items():
                d['psutil_process_' + str(k)] += v
            child_user_time = child.cpu_times().user
            child_system_time = child.cpu_times().system
            children_user_time[child.pid] = child_user_time
            children_system_time[child.pid] = child_system_time
            d['psutil_process_memory_virtual'] += child.memory_info().vms
            d['psutil_process_memory_resident'] += child.memory_info().rss
            try:
                d['psutil_process_disk_write'] += child.io_counters().write_chars
                d['psutil_process_disk_read'] += child.io_counters().read_chars
            except Exception:
                # occassionally pid temp files that hold this information are unvailable to be read so add zero
                logging.exception("Exception reading IO counters for child {k}. Recorded IO usage may be incomplete".format(k=k), exc_info=True)
                d['psutil_process_disk_write'] += 0
                d['psutil_process_disk_read'] += 0
        total_children_user_time = 0.0
        for child_pid in children_user_time:
            total_children_user_time += children_user_time[child_pid]
        total_children_system_time = 0.0
        for child_pid in children_system_time:
            total_children_system_time += children_system_time[child_pid]
        d['psutil_process_time_user'] += total_children_user_time
        d['psutil_process_time_system'] += total_children_system_time
        logging.debug("sending message")
        return d

    next_send = time.time()
    accumulate_dur = 5.0  # TODO: make configurable?

    while not terminate_event.is_set():
        logging.debug("start of monitoring loop")
        try:
            d = accumulate_and_prepare()
            if time.time() >= next_send:
                logging.debug("Sending intermediate resource message")
                radio.send((MessageType.RESOURCE_INFO, d))
                next_send += sleep_dur
        except Exception:
            logging.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)
        logging.debug("sleeping")

        # wait either until approx next send time, or the accumulation period
        # so the accumulation period will not be completely precise.
        # but before this, the sleep period was also not completely precise.
        # with a minimum floor of 0 to not upset wait

        terminate_event.wait(max(0, min(next_send - time.time(), accumulate_dur)))

    logging.debug("Sending final resource message")
    try:
        d = accumulate_and_prepare()
        radio.send((MessageType.RESOURCE_INFO, d))
    except Exception:
        logging.exception("Exception getting the resource usage. Not sending final usage to Hub", exc_info=True)
    logging.debug("End of monitoring helper")
