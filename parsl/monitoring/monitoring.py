import os
import socket
import pickle
import logging
import time
import typeguard
import datetime
import zmq

import queue
from multiprocessing import Process, Queue
from parsl.utils import RepresentationMixin
from parsl.process_loggers import wrap_with_logs

from parsl.monitoring.message_type import MessageType
from typing import Any, Callable, Dict, List, Optional, Union

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
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class UDPRadio:

    def __init__(self, monitoring_url: str, source_id: int, timeout: int = 10):
        """
        Parameters
        ----------

        monitoring_url : str
            URL of the form <scheme>://<IP>:<PORT>
        source_id : str
            String identifier of the source
        timeout : int
            timeout, default=10s
        """

        self.monitoring_url = monitoring_url
        self.sock_timeout = timeout
        self.source_id = source_id
        try:
            self.scheme, self.ip, port = (x.strip('/') for x in monitoring_url.split(':'))
            self.port = int(port)
        except Exception:
            raise Exception("Failed to parse monitoring url: {}".format(monitoring_url))

        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)  # UDP
        self.sock.settimeout(self.sock_timeout)

    def send(self, message: object) -> None:
        """ Sends a message to the UDP receiver

        Parameter
        ---------

        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            None
        """
        try:
            buffer = pickle.dumps((self.source_id,   # Identifier for manager
                                   int(time.time()),  # epoch timestamp
                                   message))
        except Exception:
            logging.exception("Exception during pickling", exc_info=True)
            return

        try:
            self.sock.sendto(buffer, (self.ip, self.port))
        except socket.timeout:
            logging.error("Could not send message within timeout limit")
            return
        return


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
                 logging_endpoint: str = 'sqlite:///monitoring.db',
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
             The specific port at which workers will be able to reach the Hub via UDP. Default: None
        hub_port_range : tuple(int, int)
             The MonitoringHub picks ports at random from the range which will be used by Hub.
             This is overridden when the hub_port option is set. Default: (55050, 56000)
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
             Set this field to True to enable logging the info of resource usage of each task. Default: True
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

    def start(self, run_id: str) -> int:

        if self.logdir is None:
            self.logdir = "."

        os.makedirs(self.logdir, exist_ok=True)

        # Initialize the ZMQ pipe to the Parsl Client
        self.logger.info("Monitoring Hub initialized")

        self.logger.debug("Initializing ZMQ Pipes to client")
        self.monitoring_hub_active = True
        self.dfk_channel_timeout = 10000  # in milliseconds
        self._context = zmq.Context()
        self._dfk_channel = self._context.socket(zmq.DEALER)
        self._dfk_channel.setsockopt(zmq.SNDTIMEO, self.dfk_channel_timeout)
        self._dfk_channel.set_hwm(0)
        self.dfk_port = self._dfk_channel.bind_to_random_port("tcp://{}".format(self.client_address),
                                                              min_port=self.client_port_range[0],
                                                              max_port=self.client_port_range[1])

        comm_q = Queue(maxsize=10)  # type: Queue[Union[Tuple[int, int], str]]
        self.exception_q = Queue(maxsize=10)  # type: Queue[Tuple[str, str]]
        self.priority_msgs = Queue()  # type: Queue[Tuple[Any, int]]
        self.resource_msgs = Queue()  # type: Queue[Tuple[Any, Any]]
        self.node_msgs = Queue()  # type: Queue[Tuple[Any, int]]
        self.block_msgs = Queue()  # type: Queue[Tuple[Any, Any]]

        self.router_proc = Process(target=router_starter,
                                   args=(comm_q, self.exception_q, self.priority_msgs, self.node_msgs, self.block_msgs, self.resource_msgs),
                                   kwargs={"hub_address": self.hub_address,
                                           "hub_port": self.hub_port,
                                           "hub_port_range": self.hub_port_range,
                                           "client_address": self.client_address,
                                           "client_port": self.dfk_port,
                                           "logdir": self.logdir,
                                           "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                           "run_id": run_id
                                   },
                                   name="Monitoring-Router-Process",
                                   daemon=True,
        )
        self.router_proc.start()

        self.dbm_proc = Process(target=dbm_starter,
                                args=(self.exception_q, self.priority_msgs, self.node_msgs, self.block_msgs, self.resource_msgs,),
                                kwargs={"logdir": self.logdir,
                                        "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                        "db_url": self.logging_endpoint,
                                  },
                                name="Monitoring-DBM-Process",
                                daemon=True,
        )
        self.dbm_proc.start()
        self.logger.info("Started the Hub process {} and DBM process {}".format(self.router_proc.pid, self.dbm_proc.pid))

        try:
            comm_q_result = comm_q.get(block=True, timeout=120)
        except queue.Empty:
            self.logger.error("Hub has not completed initialization in 120s. Aborting")
            raise Exception("Hub failed to start")

        if isinstance(comm_q_result, str):
            self.logger.error(f"MonitoringRouter sent an error message: {comm_q_result}")
            raise RuntimeError("MonitoringRouter failed to start: {comm_q_result}")

        udp_dish_port, ic_port = comm_q_result

        self.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_dish_port)
        return ic_port

    # TODO: tighten the Any message format
    def send(self, mtype: MessageType, message: Any) -> None:
        self.logger.debug("Sending message {}, {}".format(mtype, message))
        try:
            self._dfk_channel.send_pyobj((mtype, message))
        except zmq.Again:
            self.logger.exception(
                "The monitoring message sent from DFK to Hub timed-out after {}ms".format(self.dfk_channel_timeout))

    def close(self) -> None:
        if self.logger:
            self.logger.info("Terminating Monitoring Hub")
        exception_msgs = []
        while True:
            try:
                exception_msgs.append(self.exception_q.get(block=False))
                self.logger.error("There was a queued exception (Either Hub or DBM process got exception much earlier?)")
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
            self.logger.info("Waiting for Hub to receive all messages and terminate")
            self.router_proc.join()
            self.logger.debug("Finished waiting for Hub termination")
            if len(exception_msgs) == 0:
                self.priority_msgs.put(("STOP", 0))
            self.dbm_proc.join()
            self.logger.debug("Finished waiting for DBM termination")

    @staticmethod
    def monitor_wrapper(f: Any,
                        try_id: int,
                        task_id: int,
                        monitoring_hub_url: str,
                        run_id: str,
                        logging_level: int,
                        sleep_dur: float,
                        monitor_resources: bool) -> Callable:
        """ Internal
        Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
        """
        def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            # Send first message to monitoring router
            send_first_message(try_id,
                               task_id,
                               monitoring_hub_url,
                               run_id)

            if monitor_resources:
                # create the monitor process and start
                p: Optional[Process]
                p = Process(target=monitor,
                            args=(os.getpid(),
                                  try_id,
                                  task_id,
                                  monitoring_hub_url,
                                  run_id,
                                  logging_level,
                                  sleep_dur),
                            name="Monitor-Wrapper-{}".format(task_id))
                p.start()
            else:
                p = None

            try:
                return f(*args, **kwargs)
            finally:
                # There's a chance of zombification if the workers are killed by some signals
                if p:
                    p.terminate()
                    p.join()
        return wrapped


class MonitoringRouter:

    def __init__(self,
                 *,
                 hub_address: str,
                 hub_port: Optional[int] = None,
                 hub_port_range: Tuple[int, int] = (55050, 56000),

                 client_address: str = "127.0.0.1",
                 client_port: Optional[Tuple[int, int]] = None,

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
        client_address : str
             The ip address at which the dfk will be able to reach Hub. Default: "127.0.0.1"
        client_port : tuple(int, int)
             The port at which the dfk will be able to reach Hub. Default: None
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
        self.dfk_channel = self._context.socket(zmq.DEALER)
        self.dfk_channel.setsockopt(zmq.LINGER, 0)
        self.dfk_channel.set_hwm(0)
        self.dfk_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.dfk_channel.connect("tcp://{}:{}".format(client_address, client_port))

        self.ic_channel = self._context.socket(zmq.DEALER)
        self.ic_channel.setsockopt(zmq.LINGER, 0)
        self.ic_channel.set_hwm(0)
        self.ic_channel.RCVTIMEO = int(self.loop_freq)  # in milliseconds
        self.logger.debug("hub_address: {}. hub_port_range {}".format(hub_address, hub_port_range))
        self.ic_port = self.ic_channel.bind_to_random_port("tcp://*",
                                                           min_port=hub_port_range[0],
                                                           max_port=hub_port_range[1])

    def start(self,
              priority_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
              node_msgs: "queue.Queue[Tuple[Dict[str, Any], int]]",
              block_msgs: "queue.Queue[Tuple[Dict[str, Any], int]]",
              resource_msgs: "queue.Queue[Tuple[Dict[str, Any], str]]") -> None:
        try:
            while True:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    resource_msgs.put((msg, addr))
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                except socket.timeout:
                    pass

                try:
                    msg = self.dfk_channel.recv_pyobj()
                    self.logger.debug("Got ZMQ Message from DFK: {}".format(msg))
                    if msg[0].value == MessageType.BLOCK_INFO.value:
                        block_msgs.put((msg, 0))
                    else:
                        priority_msgs.put((msg, 0))
                    if msg[0].value == MessageType.WORKFLOW_INFO.value and 'python_version' not in msg[1]:
                        break
                except zmq.Again:
                    pass
                except Exception:
                    # This will catch malformed messages. What happens if the
                    # dfk_channel is broken in such a way that it always raises
                    # an exception? Looping on this would maybe be the wrong
                    # thing to do.
                    self.logger.warning("Failure processing a DFK ZMQ message", exc_info=True)

                try:
                    msg = self.ic_channel.recv_pyobj()
                    self.logger.debug("Got ZMQ Message from interchange: {}".format(msg))
                    if msg[0].value == MessageType.NODE_INFO.value:
                        msg[2]['last_heartbeat'] = datetime.datetime.fromtimestamp(msg[2]['last_heartbeat'])
                        msg[2]['run_id'] = self.run_id
                        msg[2]['timestamp'] = msg[1]
                        msg = (msg[0], msg[2])
                        node_msgs.put((msg, 0))
                    elif msg[0].value == MessageType.BLOCK_INFO.value:
                        block_msgs.put((msg, 0))
                    else:
                        self.logger.error(f"Discarding message from interchange with unknown type {msg[0].value}")
                except zmq.Again:
                    pass

            self.logger.info("Monitoring router draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    resource_msgs.put((msg, addr))
                    last_msg_received_time = time.time()
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                except socket.timeout:
                    pass

            self.logger.info("Monitoring router finishing normally")
        finally:
            self.logger.info("Monitoring router finished")


@wrap_with_logs
def router_starter(comm_q: "queue.Queue[Union[Tuple[int, int], str]]",
                   exception_q: "queue.Queue[Tuple[str, str]]",
                   priority_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
                   node_msgs: "queue.Queue[Tuple[Dict[str, Any], int]]",
                   block_msgs: "queue.Queue[Tuple[Dict[str, Any], int]]",
                   resource_msgs: "queue.Queue[Tuple[Dict[str, Any], str]]",

                   hub_address: str,
                   hub_port: Optional[int],
                   hub_port_range: Tuple[int, int],

                   client_address: str,
                   client_port: Optional[Tuple[int, int]],

                   logdir: str,
                   logging_level: int,
                   run_id: str) -> None:

    try:
        router = MonitoringRouter(hub_address=hub_address,
                                  hub_port=hub_port,
                                  hub_port_range=hub_port_range,
                                  client_address=client_address,
                                  client_port=client_port,
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
                       run_id: str) -> None:
    import platform

    radio = UDPRadio(monitoring_hub_url,
                     source_id=task_id)

    msg = {'run_id': run_id,
           'try_id': try_id,
           'task_id': task_id,
           'hostname': platform.node(),
           'first_msg': True,
           'timestamp': datetime.datetime.now()
    }
    radio.send(msg)
    return


@wrap_with_logs
def monitor(pid: int,
            try_id: int,
            task_id: int,
            monitoring_hub_url: str,
            run_id: str,
            logging_level: int = logging.INFO,
            sleep_dur: float = 10) -> None:
    """Internal
    Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.
    """
    import logging
    import platform
    import psutil
    import time

    radio = UDPRadio(monitoring_hub_url,
                     source_id=task_id)

    format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"
    logging.basicConfig(filename='{logbase}/monitor.{task_id}.{pid}.log'.format(
        logbase="/tmp", task_id=task_id, pid=pid), level=logging_level, format=format_string)

    logging.debug("start of monitor")

    # these values are simple to log. Other information is available in special formats such as memory below.
    simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    # values that can be summed up to see total resources used by task process and its children
    summable_values = ['cpu_percent', 'memory_percent', 'num_threads']

    pm = psutil.Process(pid)
    pm.cpu_percent()

    children_user_time = {}  # type: Dict[int, float]
    children_system_time = {}  # type: Dict[int, float]
    total_children_user_time = 0.0
    total_children_system_time = 0.0
    while True:
        logging.debug("start of monitoring loop")
        try:
            d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
            d["run_id"] = run_id
            d["task_id"] = task_id
            d["try_id"] = try_id
            d['resource_monitoring_interval'] = sleep_dur
            d['hostname'] = platform.node()
            d['first_msg'] = False
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
                d['psutil_process_disk_write'] = pm.io_counters().write_bytes
                d['psutil_process_disk_read'] = pm.io_counters().read_bytes
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
                total_children_user_time += child_user_time - children_user_time.get(child.pid, 0)
                total_children_system_time += child_system_time - children_system_time.get(child.pid, 0)
                children_user_time[child.pid] = child_user_time
                children_system_time[child.pid] = child_system_time
                d['psutil_process_memory_virtual'] += child.memory_info().vms
                d['psutil_process_memory_resident'] += child.memory_info().rss
                try:
                    d['psutil_process_disk_write'] += child.io_counters().write_bytes
                    d['psutil_process_disk_read'] += child.io_counters().read_bytes
                except Exception:
                    # occassionally pid temp files that hold this information are unvailable to be read so add zero
                    logging.exception("Exception reading IO counters for child {k}. Recorded IO usage may be incomplete".format(k=k), exc_info=True)
                    d['psutil_process_disk_write'] += 0
                    d['psutil_process_disk_read'] += 0
            d['psutil_process_time_user'] += total_children_user_time
            d['psutil_process_time_system'] += total_children_system_time
            logging.debug("sending message")
            radio.send(d)
        except Exception:
            logging.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)

        logging.debug("sleeping")
        time.sleep(sleep_dur)
