import os
import socket
import pickle
import logging
import time
import typeguard
import datetime
import zmq

import queue
from abc import ABCMeta, abstractmethod
from parsl.multiprocessing import ForkProcess, SizedQueue
from multiprocessing import Process, Queue
from parsl.utils import RepresentationMixin
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.serialize import deserialize

# this is needed for htex hack to get at htex result queue
import parsl.executors.high_throughput.monitoring_info

from parsl.monitoring.message_type import MessageType
from typing import cast, Any, Callable, Dict, List, Optional, Union

from parsl.serialize import serialize

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


class MonitoringRadio(metaclass=ABCMeta):
    @abstractmethod
    def send(self, message: object) -> None:
        pass


class FilesystemRadio(MonitoringRadio):
    def __init__(self, *, monitoring_hub_url: str, source_id: int, timeout: int = 10, run_dir: str):
        logger.info("filesystem based monitoring channel initializing")
        self.source_id = source_id
        self.id_counter = 0
        self.radio_uid = f"host-{socket.gethostname()}-pid-{os.getpid()}-radio-{id(self)}"
        self.base_path = f"{run_dir}/monitor-fs-radio/"

    def send(self, message: object) -> None:
        logger.info("Sending a monitoring message via filesystem")

        tmp_path = f"{self.base_path}/tmp"
        new_path = f"{self.base_path}/new"

        # this should be randomised by things like worker ID, process ID, whatever
        # because there will in general be many FilesystemRadio objects sharing the
        # same space (even from the same process). id(self) used here will
        # disambiguate in one process at one instant, but not between
        # other things: eg different hosts, different processes, same process different non-overlapping instantiations
        unique_id = f"msg-{self.radio_uid}-{self.id_counter}"

        self.id_counter = self.id_counter + 1

        # TODO: use path operators not string interpolation
        tmp_filename = f"{tmp_path}/{unique_id}"
        new_filename = f"{new_path}/{unique_id}"
        buffer = (message, "NA")

        # this will write the message out then atomically
        # move it into new/, so that a partially written
        # file will never be observed in new/
        with open(tmp_filename, "wb") as f:
            f.write(serialize(buffer))
        os.rename(tmp_filename, new_filename)


class HTEXRadio(MonitoringRadio):

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
        self.source_id = source_id
        logger.info("htex-based monitoring channel initialising")

    def send(self, message: object) -> None:
        """ Sends a message to the UDP receiver

        Parameter
        ---------

        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            None
        """
        # TODO: this message needs to look like the other messages that the interchange will send...
        #            hub_channel.send_pyobj((MessageType.NODE_INFO,
        #                            datetime.datetime.now(),
        #                            self._ready_manager_queue[manager]))

        # not serialising here because it looks like python objects can go through mp queues without explicit pickling?
        try:
            buffer = message
        except Exception:
            logging.exception("Exception during pickling", exc_info=True)
            return

        result_queue = parsl.executors.high_throughput.monitoring_info.result_queue

        # this message needs to go in the result queue tagged so that it is treated
        # i) as a monitoring message by the interchange, and then further more treated
        # as a RESOURCE_INFO message when received by monitoring (rather than a NODE_INFO
        # which is the implicit default for messages from the interchange)

        # for the interchange, the outer wrapper, this needs to be a dict:

        interchange_msg = {
            'type': 'monitoring',
            'payload': buffer
        }

        if result_queue:
            result_queue.put(pickle.dumps(interchange_msg))
        else:
            logger.error("result_queue is uninitialized - cannot put monitoring message")

        return


class UDPRadio(MonitoringRadio):

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
            buffer = pickle.dumps(message)
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
        self.resource_msgs = SizedQueue()  # type: Queue[Tuple[Tuple[MessageType, Dict[str, Any]], Any]]
        self.node_msgs = SizedQueue()  # type: Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]
        self.block_msgs = SizedQueue()  # type:  Queue[Tuple[Tuple[MessageType, Dict[str, Any]], Any]]

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
        self.logger.info("Started the Hub process {} and DBM process {}".format(self.router_proc.pid, self.dbm_proc.pid))

        self.filesystem_proc = Process(target=filesystem_receiver,
                                       args=(self.logdir, self.resource_msgs, run_dir),
                                       name="Monitoring-Filesystem-Process",
                                       daemon=True)
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
        self.logger.debug("Sending message {}, {}".format(mtype, message))
        try:
            self._dfk_channel.send_pyobj((mtype, message))
        except zmq.Again:
            self.logger.exception(
                "The monitoring message sent from DFK to Hub timed-out after {}ms".format(self.dfk_channel_timeout))
        else:
            self.logger.debug("Sent message {}, {}".format(mtype, message))

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
                self.filesystem_proc.terminate()
            self.logger.info("Waiting for Hub to receive all messages and terminate")
            self.router_proc.join()
            self.logger.debug("Finished waiting for Hub termination")
            if len(exception_msgs) == 0:
                self.priority_msgs.put(("STOP", 0))
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
        """ Internal
        Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
        """
        def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            logger.debug("wrapped: 1. start of wrapped")
            # Send first message to monitoring router
            send_first_message(try_id,
                               task_id,
                               monitoring_hub_url,
                               run_id,
                               radio_mode,
                               run_dir)
            logger.debug("wrapped: 2. sent first message")

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
                                       sleep_dur, run_dir),
                                 name="Monitor-Wrapper-{}".format(task_id))
                logger.debug("wrapped: 3. created monitor process, pid {}".format(pp.pid))
                pp.start()
                logger.debug("wrapped: 4. started monitor process, pid {}".format(pp.pid))
                p = pp
                #  TODO: awkwardness because ForkProcess is not directly a constructor
                # and type-checking is expecting p to be optional and cannot
                # narrow down the type of p in this block.

            else:
                p = None

            try:
                logger.debug("wrapped: 5. invoking wrapped function")
                r = f(*args, **kwargs)
                logger.debug("wrapped: 6. back from wrapped function ok")
                return r
            finally:
                logger.debug("wrapped: 10 in 2nd finally")
                logger.debug("wrapped: 10.1 sending last message")
                send_last_message(try_id,
                                  task_id,
                                  monitoring_hub_url,
                                  run_id,
                                  radio_mode, run_dir)
                logger.debug("wrapped: 10.1 sent last message")
                # There's a chance of zombification if the workers are killed by some signals
                if p:
                    p.terminate()
                    logger.debug("wrapped: 11 done terminating monitor")
                    p.join()
                    logger.debug("wrapped: 12 done joining monitor again")
        return wrapped


# this needs proper typing, but I was having some problems with typeguard...
@wrap_with_logs
def filesystem_receiver(logdir: str, q: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], Any]]", run_dir: str) -> None:
    logger = start_file_logger("{}/monitoring_filesystem_radio.log".format(logdir),
                               name="monitoring_filesystem_radio",
                               level=logging.DEBUG)

    logger.info("Starting filesystem radio receiver")
    setproctitle("parsl: monitoring filesystem receiver")
    # TODO: these paths should be created by path tools, not f-strings
    # likewise the other places where tmp_dir, new_dir are created on
    # the sending side.
    base_path = f"{run_dir}/monitor-fs-radio/"
    tmp_dir = f"{base_path}/tmp/"
    new_dir = f"{base_path}/new/"
    logger.debug("Creating new and tmp paths")

    os.makedirs(tmp_dir)
    os.makedirs(new_dir)

    while True:  # needs an exit condition, that also copes with late messages
        # like the UDP radio receiver.
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
                q.put(cast(Any, message))  # TODO: sort this typing/cast out
                # should this addr field at the end be removed? does it ever
                # get used in monitoring?
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
              priority_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
              node_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
              block_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
              resource_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], Any]]") -> None:
        try:
            router_keep_going = True
            while router_keep_going:
                try:
                    data, addr = self.sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    self.logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                    resource_msgs.put((msg, addr))
                except socket.timeout:
                    pass

                try:
                    dfk_loop_start = time.time()
                    while time.time() - dfk_loop_start < 1.0:  # TODO make configurable
                        msg = self.ic_channel.recv_pyobj()
                        self.logger.debug("Got ZMQ Message: {}".format(msg))
                        assert isinstance(msg, tuple), "IC Channel expects only tuples, got {}".format(msg)
                        assert len(msg) >= 1, "IC Channel expects tuples of length at least 1, got {}".format(msg)
                        if msg[0] == MessageType.NODE_INFO:
                            self.logger.info("message is NODE_INFO")
                            assert len(msg) == 2, "IC Channel expects NODE_INFO tuples of length 2, got {}".format(msg)
                            msg[1]['run_id'] = self.run_id

                            # ((tag, dict), addr)
                            node_msg = (msg, 0)
                            node_msgs.put(cast(Any, node_msg))
                        elif msg[0] == MessageType.RESOURCE_INFO:
                            # with more uniform handling of messaging, it doesn't matter
                            # too much which queue this goes to now... could be node_msgs
                            # just as well, I think.
                            # and if the above message rewriting was got rid of, this block might not need to switch on message tag at all.
                            self.logger.info("Handling as RESOURCE_INFO")
                            resource_msgs.put(cast(Any, (msg, 0)))
                        elif msg[0] == MessageType.BLOCK_INFO:
                            self.logger.info("Putting message to block_msgs: {}".format((msg, 0)))
                            # block_msgs.put((msg, 0))
                            block_msgs.put(cast(Any, (msg, 0)))
                            # TODO this cast is suspicious and is to make mypy
                            # trivially pass rather than me paying attention to
                            # the message structure. so if something breaks in
                            # this patch, it could well be here.
                        elif msg[0] == MessageType.TASK_INFO:
                            priority_msgs.put((cast(Any, msg), 0))
                        elif msg[0] == MessageType.WORKFLOW_INFO:
                            priority_msgs.put((cast(Any, msg), 0))
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
            # TODO: should this drain loop deal with all the connections, or just the UDP drain? I think all of them.
            # it might just be chance that we're not losing (or not noticing lost) data here?
            # so that main loop ^ could be refactored into a normal+draining exit condition
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
                   priority_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
                   node_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
                   block_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], int]]",
                   resource_msgs: "queue.Queue[Tuple[Tuple[MessageType, Dict[str, Any]], str]]",

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
        radio = FilesystemRadio(monitoring_hub_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': True,
            'last_msg': False,
            'timestamp': datetime.datetime.now()
    })
    radio.send(msg)
    return


# TODO: factor with send_first_message
@wrap_with_logs
def send_last_message(try_id: int,
                      task_id: int,
                      monitoring_hub_url: str,
                      run_id: str, radio_mode: str, run_dir: str) -> None:
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
        radio = FilesystemRadio(monitoring_hub_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': False,
            'last_msg': True,
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
            logging_level: int = logging.INFO,
            sleep_dur: float = 10, run_dir: str = "./") -> None:
    """Internal
    Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.
    """
    import logging
    import platform
    import psutil
    import time

    radio: MonitoringRadio
    if radio_mode == "udp":
        radio = UDPRadio(monitoring_hub_url,
                         source_id=task_id)
    elif radio_mode == "htex":
        radio = HTEXRadio(monitoring_hub_url,
                          source_id=task_id)
    elif radio_mode == "filesystem":
        radio = FilesystemRadio(monitoring_hub_url=monitoring_hub_url,
                                source_id=task_id, run_dir=run_dir)
    else:
        raise RuntimeError(f"Unknown radio mode: {radio_mode}")

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
            radio.send((MessageType.RESOURCE_INFO, d))
        except Exception:
            logging.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)

        logging.debug("sleeping")
        time.sleep(sleep_dur)
