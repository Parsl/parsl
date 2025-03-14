from __future__ import annotations

import logging
import multiprocessing.synchronize as ms
import os
import warnings
from multiprocessing import Event
from multiprocessing.context import ForkProcess as ForkProcessType
from multiprocessing.queues import Queue
from typing import Any, Optional, Union

import typeguard

from parsl.monitoring.radios.base import RadioConfig
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.monitoring.types import TaggedMonitoringMessage
from parsl.multiprocessing import ForkProcess, SizedQueue
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
                 # TODO: hub port deprecation here
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
             unused - describe, with end date
        TODO: where's the hub_port_range param gone?
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

        self.resource_msgs: Queue[TaggedMonitoringMessage]
        self.resource_msgs = SizedQueue()

        self.router_exit_event: ms.Event
        self.router_exit_event = Event()

        self.dbm_exit_event: ms.Event
        self.dbm_exit_event = Event()

        self.dbm_proc = ForkProcess(target=dbm_starter,
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

        self.radio = MultiprocessingQueueRadioSender(self.resource_msgs)

        # need to initialize radio configs, perhaps first time a radio config is used
        # in each executor? (can't do that at startup because executor list is dynamic,
        # don't know all the executors till later)
        # self.radio_config.monitoring_hub_url = "udp://{}:{}".format(self.hub_address, udp_port)
        # How can this config be populated properly?
        # There's a UDP port chosen right now by the monitoring router and
        # sent back a line above...
        # What does that look like for other radios? htexradio has no specific config at all,
        # filesystem radio has a path (that should have been created?) for config, and a loop
        # that needs to be running, started in this start method.
        # so something like... radio_config.receive() generates the appropriate receiver object?
        # which has a shutdown method on it for later. and also updates radio_config itself so
        # it has the right info to send across the wire? or some state driving like that?

        logger.info("Monitoring Hub initialized")

    def send(self, message: TaggedMonitoringMessage) -> None:
        logger.debug("Sending message type %s", message[0])
        self.radio.send(message)

    def close(self) -> None:
        logger.info("Terminating Monitoring Hub")
        if self.monitoring_hub_active:
            self.monitoring_hub_active = False
            logger.info("Setting router termination event")
            self.router_exit_event.set()

            logger.debug("Waiting for DB termination")
            self.dbm_exit_event.set()
            join_terminate_close_proc(self.dbm_proc)
            logger.debug("Finished waiting for DBM termination")

            logger.info("Closing monitoring multiprocessing queues")
            self.resource_msgs.close()
            self.resource_msgs.join_thread()
            logger.info("Closed monitoring multiprocessing queues")

    def start_receiver(self, radio_config: RadioConfig, ip: str, run_dir: str) -> Any:
        """somehow start a radio receiver here and update radioconfig to be sent over the wire, without
        losing the info we need to shut down that receiver later...
        """
        r = radio_config.create_receiver(ip=ip, run_dir=run_dir, resource_msgs=self.resource_msgs)
        logger.info(f"BENC: created receiver {r}")
        # assert r is not None
        return r
        # ... that is, a thing we need to do a shutdown call on at shutdown, a "shutdownable"? without
        # expecting any more structure on it?


def join_terminate_close_proc(process: ForkProcessType, *, timeout: int = 30) -> None:
    """Increasingly aggressively terminate a process.

    This function assumes that the process is likely to exit before
    the join timeout, driven by some other means, such as the
    MonitoringHub router_exit_event. If the process does not exit, then
    first terminate() and then kill() will be used to end the process.

    In the case of a very mis-behaving process, this function might take
    up to 3*timeout to exhaust all termination methods and return.
    """
    logger.debug("Joining process")
    process.join(timeout)

    # run a sequence of increasingly aggressive steps to shut down the process.
    if process.is_alive():
        logger.error("Process did not join. Terminating.")
        process.terminate()
        process.join(timeout)
        if process.is_alive():
            logger.error("Process did not join after terminate. Killing.")
            process.kill()
            process.join(timeout)
            # This kill should not be caught by any signal handlers so it is
            # unlikely that this join will timeout. If it does, there isn't
            # anything further to do except log an error in the next if-block.

    if process.is_alive():
        logger.error("Process failed to end")
        # don't call close if the process hasn't ended:
        # process.close() doesn't work on a running process.
    else:
        process.close()
