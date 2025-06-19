from __future__ import annotations

import logging
import multiprocessing.synchronize as ms
import os
import warnings
from multiprocessing.queues import Queue
from typing import Any, Optional, Union

import typeguard

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
                 hub_address: Any = None,  # unused, so no type enforcement
                 hub_port_range: Any = None,  # unused, so no type enforcement
                 hub_port: Any = None,  # unused, so no type enforcement

                 workflow_name: Optional[str] = None,
                 workflow_version: Optional[str] = None,
                 logging_endpoint: Optional[str] = None,
                 monitoring_debug: bool = False,
                 resource_monitoring_enabled: bool = True,
                 resource_monitoring_interval: float = 30):  # in seconds
        """
        Parameters
        ----------
        hub_address : unused
        hub_port : unused
             Unused, but probably retained until 2026-06-01 to give deprecation warning.
             These two values previously configured UDP parameters when UDP was used
             for monitoring messages from workers. These are now configured on the
             relevant UDPRadio.
        hub_port_range : unused
             Unused, but probably retained until 2026-06-01 to give deprecation warning.
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

        # The following three parameters need to exist as attributes to be
        # output by RepresentationMixin.
        if hub_address is not None:
            message = "Instead of MonitoringHub.hub_address, specify UDPRadio(address=...)"
            warnings.warn(message, DeprecationWarning)
            logger.warning(message)

        self.hub_address = hub_address

        if hub_port is not None:
            message = "Instead of MonitoringHub.hub_port, specify UDPRadio(port=...)"
            warnings.warn(message, DeprecationWarning)
            logger.warning(message)

        self.hub_port = hub_port

        if hub_port_range is not None:
            message = "Instead of MonitoringHub.hub_port_range, Use HighThroughputExecutor.interchange_port_range"
            warnings.warn(message, DeprecationWarning)
            logger.warning(message)

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

        self.resource_msgs: Queue[TaggedMonitoringMessage]
        self.resource_msgs = SizedQueue()

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
        logger.info("Started DBM process %s", self.dbm_proc.pid)
        logger.info("Monitoring Hub initialized")

    def close(self) -> None:
        logger.info("Terminating Monitoring Hub")
        if self.monitoring_hub_active:
            self.monitoring_hub_active = False
            logger.debug("Waiting for DB termination")
            self.dbm_exit_event.set()
            join_terminate_close_proc(self.dbm_proc)
            logger.debug("Finished waiting for DBM termination")

            logger.info("Closing monitoring multiprocessing queues")
            self.resource_msgs.close()
            self.resource_msgs.join_thread()
            logger.info("Closed monitoring multiprocessing queues")
