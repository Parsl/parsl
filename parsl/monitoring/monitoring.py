from __future__ import annotations

import logging
import multiprocessing.synchronize as ms
import os
import queue
import warnings
from multiprocessing.queues import Queue
from typing import TYPE_CHECKING, Any, Optional, Union

import typeguard

from parsl.monitoring.errors import MonitoringHubStartError
from parsl.monitoring.radios.filesystem_router import filesystem_router_starter
from parsl.monitoring.radios.udp_router import udp_router_starter
from parsl.monitoring.types import TaggedMonitoringMessage
from parsl.multiprocessing import (
    SizedQueue,
    SpawnEvent,
    SpawnProcess,
    join_terminate_close_proc,
)
from parsl.utils import RepresentationMixin

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
                 hub_port_range: Any = None,

                 workflow_name: Optional[str] = None,
                 workflow_version: Optional[str] = None,
                 logging_endpoint: Optional[str] = None,
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
        hub_port_range : unused
             Unused, but retained until 2025-09-14 to avoid configuration errors.
             This value previously configured one ZMQ channel inside the
             HighThroughputExecutor. That ZMQ channel is now configured by the
             interchange_port_range parameter of HighThroughputExecutor.
        workflow_name : str
             The name for the workflow. Default to the name of the parsl script
        workflow_version : str
             The version of the workflow. Default to the beginning datetime of the parsl script
        logging_endpoint : str
             The database connection url for monitoring to log the information.
             These URLs follow RFC-1738, and can include username, password, hostname, database name.
             Default: sqlite, in the configured run_dir.
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

        if hub_port_range is not None:
            message = "Instead of MonitoringHub.hub_port_range, Use HighThroughputExecutor.interchange_port_range"
            warnings.warn(message, DeprecationWarning)
            logger.warning(message)
        # This is used by RepresentationMixin so needs to exist as an attribute
        # even though now it is otherwise unused.
        self.hub_port_range = hub_port_range

        self.logging_endpoint = logging_endpoint
        self.monitoring_debug = monitoring_debug

        self.workflow_name = workflow_name
        self.workflow_version = workflow_version

        self.resource_monitoring_enabled = resource_monitoring_enabled
        self.resource_monitoring_interval = resource_monitoring_interval

    def start(self, dfk_run_dir: str, config_run_dir: Union[str, os.PathLike]) -> None:

        logger.debug("Starting MonitoringHub")

        if self.logging_endpoint is None:
            self.logging_endpoint = f"sqlite:///{os.fspath(config_run_dir)}/monitoring.db"

        os.makedirs(dfk_run_dir, exist_ok=True)

        self.monitoring_hub_active = True

        # This annotation is incompatible with typeguard 4.x instrumentation
        # of local variables: Queue is not subscriptable at runtime, as far
        # as typeguard is concerned. The more general Queue annotation works,
        # but does not restrict the contents of the Queue. Using TYPE_CHECKING
        # here allows the stricter definition to be seen by mypy, and the
        # simpler definition to be seen by typeguard. Hopefully at some point
        # in the future, Queue will allow runtime subscripts.

        if TYPE_CHECKING:
            udp_comm_q: Queue[Union[int, str]]
        else:
            udp_comm_q: Queue

        udp_comm_q = SizedQueue(maxsize=10)

        self.resource_msgs: Queue[TaggedMonitoringMessage]
        self.resource_msgs = SizedQueue()

        self.router_exit_event: ms.Event
        self.router_exit_event = SpawnEvent()

        self.udp_router_proc = SpawnProcess(target=udp_router_starter,
                                            kwargs={"comm_q": udp_comm_q,
                                                    "resource_msgs": self.resource_msgs,
                                                    "exit_event": self.router_exit_event,
                                                    "udp_port": self.hub_port,
                                                    "run_dir": dfk_run_dir,
                                                    "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                                    },
                                            name="Monitoring-UDP-Router-Process",
                                            daemon=True,
                                            )
        self.udp_router_proc.start()

        self.dbm_exit_event: ms.Event
        self.dbm_exit_event = SpawnEvent()

        self.dbm_proc = SpawnProcess(target=dbm_starter,
                                     args=(self.resource_msgs,),
                                     kwargs={"run_dir": dfk_run_dir,
                                             "logging_level": logging.DEBUG if self.monitoring_debug else logging.INFO,
                                             "db_url": self.logging_endpoint,
                                             "exit_event": self.dbm_exit_event,
                                             },
                                     name="Monitoring-DBM-Process",
                                     daemon=True,
                                     )
        self.dbm_proc.start()
        logger.info("Started UDP router process %s and DBM process %s",
                    self.udp_router_proc.pid, self.dbm_proc.pid)

        self.filesystem_proc = SpawnProcess(target=filesystem_router_starter,
                                            kwargs={"q": self.resource_msgs,
                                                    "run_dir": dfk_run_dir,
                                                    "exit_event": self.router_exit_event},
                                            name="Monitoring-Filesystem-Process",
                                            daemon=True
                                            )
        self.filesystem_proc.start()
        logger.info("Started filesystem radio receiver process %s", self.filesystem_proc.pid)

        try:
            udp_comm_q_result = udp_comm_q.get(block=True, timeout=120)
            udp_comm_q.close()
            udp_comm_q.join_thread()
        except queue.Empty:
            logger.error("Monitoring UDP router has not reported port in 120s. Aborting")
            raise MonitoringHubStartError()

        if isinstance(udp_comm_q_result, str):
            logger.error("MonitoringRouter sent an error message: %s", udp_comm_q_result)
            raise RuntimeError(f"MonitoringRouter failed to start: {udp_comm_q_result}")

        udp_port = udp_comm_q_result
        self.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_port)

        logger.info("Monitoring Hub initialized")

    def close(self) -> None:
        logger.info("Terminating Monitoring Hub")
        if self.monitoring_hub_active:
            self.monitoring_hub_active = False
            logger.info("Setting router termination event")
            self.router_exit_event.set()

            logger.info("Waiting for UDP router to terminate")
            join_terminate_close_proc(self.udp_router_proc)

            logger.debug("Finished waiting for router termination")
            logger.debug("Waiting for DB termination")
            self.dbm_exit_event.set()
            join_terminate_close_proc(self.dbm_proc)
            logger.debug("Finished waiting for DBM termination")

            logger.info("Terminating filesystem radio receiver process")
            join_terminate_close_proc(self.filesystem_proc)

            logger.info("Closing monitoring multiprocessing queues")
            self.resource_msgs.close()
            self.resource_msgs.join_thread()
            logger.info("Closed monitoring multiprocessing queues")
