from __future__ import annotations

import os
import time
import logging
import multiprocessing.synchronize as ms
import typeguard

import queue

from parsl.multiprocessing import ForkProcess, SizedQueue
from multiprocessing import Process
from multiprocessing import Event
from multiprocessing.queues import Queue
from parsl.log_utils import set_file_logger
from parsl.utils import RepresentationMixin
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.serialize import deserialize

from parsl.monitoring.radios import MultiprocessingQueueRadio
from parsl.monitoring.router import router_starter
from parsl.monitoring.message_type import MessageType
from parsl.monitoring.types import AddressedMonitoringMessage
from typing import cast, Any, Optional, Tuple, Union, TYPE_CHECKING

_db_manager_excepts: Optional[Exception]


try:
    from parsl.monitoring.db_manager import dbm_starter
except Exception as e:
    _db_manager_excepts = e
else:
    _db_manager_excepts = None

logger = logging.getLogger(__name__)


@typeguard.typechecked
class MonitoringHub(RepresentationMixin):
    def __init__(self,
                 hub_address: str,
                 hub_port: Optional[int] = None,
                 hub_port_range: Tuple[int, int] = (55050, 56000),

                 workflow_name: Optional[str] = None,
                 workflow_version: Optional[str] = None,
                 logging_endpoint: Optional[str] = None,
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
        workflow_name : str
             The name for the workflow. Default to the name of the parsl script
        workflow_version : str
             The version of the workflow. Default to the beginning datetime of the parsl script
        logging_endpoint : str
             The database connection url for monitoring to log the information.
             These URLs follow RFC-1738, and can include username, password, hostname, database name.
             Default: sqlite, in the configured run_dir.
        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'
        monitoring_debug : Bool
             Enable monitoring debug logging. Default: False
        resource_monitoring_enabled : boolean
             Set this field to True to enable logging of information from the worker side.
             This will include environment information such as start time, hostname and block id,
             along with periodic resource usage of each task. Default: True
        resource_monitoring_interval : float
             The time interval, in seconds, at which the monitoring records the resource usage of each task.
             If set to 0, only start and end information will be logged, and no periodic monitoring will
             be made.
             Default: 30 seconds
        """

        if _db_manager_excepts:
            raise _db_manager_excepts

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

    def start(self, run_id: str, dfk_run_dir: str, config_run_dir: Union[str, os.PathLike]) -> int:

        logger.debug("Starting MonitoringHub")

        if self.logdir is None:
            self.logdir = "."

        if self.logging_endpoint is None:
            self.logging_endpoint = f"sqlite:///{os.fspath(config_run_dir)}/monitoring.db"

        os.makedirs(self.logdir, exist_ok=True)

        self.monitoring_hub_active = True

        # This annotation is incompatible with typeguard 4.x instrumentation
        # of local variables: Queue is not subscriptable at runtime, as far
        # as typeguard is concerned. The more general Queue annotation works,
        # but does not restrict the contents of the Queue. Using TYPE_CHECKING
        # here allows the stricter definition to be seen by mypy, and the
        # simpler definition to be seen by typeguard. Hopefully at some point
        # in the future, Queue will allow runtime subscripts.

        if TYPE_CHECKING:
            comm_q: Queue[Union[Tuple[int, int], str]]
        else:
            comm_q: Queue

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

        self.router_exit_event: ms.Event
        self.router_exit_event = Event()

        self.router_proc = ForkProcess(target=router_starter,
                                       args=(comm_q, self.exception_q, self.priority_msgs, self.node_msgs,
                                             self.block_msgs, self.resource_msgs, self.router_exit_event),
                                       kwargs={"hub_address": self.hub_address,
                                               "udp_port": self.hub_port,
                                               "zmq_port_range": self.hub_port_range,
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
        logger.info("Started the router process {} and DBM process {}".format(self.router_proc.pid, self.dbm_proc.pid))

        self.filesystem_proc = Process(target=filesystem_receiver,
                                       args=(self.logdir, self.resource_msgs, dfk_run_dir),
                                       name="Monitoring-Filesystem-Process",
                                       daemon=True
                                       )
        self.filesystem_proc.start()
        logger.info(f"Started filesystem radio receiver process {self.filesystem_proc.pid}")

        self.radio = MultiprocessingQueueRadio(self.block_msgs)

        try:
            comm_q_result = comm_q.get(block=True, timeout=120)
            comm_q.close()
            comm_q.join_thread()
        except queue.Empty:
            logger.error("Hub has not completed initialization in 120s. Aborting")
            raise Exception("Hub failed to start")

        if isinstance(comm_q_result, str):
            logger.error(f"MonitoringRouter sent an error message: {comm_q_result}")
            raise RuntimeError(f"MonitoringRouter failed to start: {comm_q_result}")

        udp_port, zmq_port = comm_q_result

        self.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_port)

        logger.info("Monitoring Hub initialized")

        return zmq_port

    # TODO: tighten the Any message format
    def send(self, mtype: MessageType, message: Any) -> None:
        logger.debug("Sending message type {}".format(mtype))
        self.radio.send((mtype, message))

    def close(self) -> None:
        logger.info("Terminating Monitoring Hub")
        exception_msgs = []
        while True:
            try:
                exception_msgs.append(self.exception_q.get(block=False))
                logger.error("There was a queued exception (Either router or DBM process got exception much earlier?)")
            except queue.Empty:
                break
        if self.monitoring_hub_active:
            self.monitoring_hub_active = False
            if exception_msgs:
                for exception_msg in exception_msgs:
                    logger.error(
                        "{} process delivered an exception: {}. Terminating all monitoring processes immediately.".format(
                            exception_msg[0],
                            exception_msg[1]
                        )
                    )
                self.router_proc.terminate()
                self.dbm_proc.terminate()
                self.filesystem_proc.terminate()
            logger.info("Setting router termination event")
            self.router_exit_event.set()
            logger.info("Waiting for router to terminate")
            self.router_proc.join()
            logger.debug("Finished waiting for router termination")
            if len(exception_msgs) == 0:
                logger.debug("Sending STOP to DBM")
                self.priority_msgs.put(("STOP", 0))
            else:
                logger.debug("Not sending STOP to DBM, because there were DBM exceptions")
            logger.debug("Waiting for DB termination")
            self.dbm_proc.join()
            logger.debug("Finished waiting for DBM termination")

            # should this be message based? it probably doesn't need to be if
            # we believe we've received all messages
            logger.info("Terminating filesystem radio receiver process")
            self.filesystem_proc.terminate()
            self.filesystem_proc.join()

            logger.info("Closing monitoring multiprocessing queues")
            self.exception_q.close()
            self.exception_q.join_thread()
            self.priority_msgs.close()
            self.priority_msgs.join_thread()
            self.resource_msgs.close()
            self.resource_msgs.join_thread()
            self.node_msgs.close()
            self.node_msgs.join_thread()
            self.block_msgs.close()
            self.block_msgs.join_thread()
            logger.info("Closed monitoring multiprocessing queues")


@wrap_with_logs
def filesystem_receiver(logdir: str, q: "queue.Queue[AddressedMonitoringMessage]", run_dir: str) -> None:
    logger = set_file_logger("{}/monitoring_filesystem_radio.log".format(logdir),
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
