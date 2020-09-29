import logging
import parsl  # noqa F401 (used in string type annotation)
import time
import zmq
from typing import Dict, Sequence
from typing import List  # noqa F401 (used in type annotation)

from parsl.dataflow.executor_status import ExecutorStatus
from parsl.dataflow.job_error_handler import JobErrorHandler
from parsl.dataflow.strategy import Strategy
from parsl.executors.base import ParslExecutor
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.message_type import MessageType

from parsl.providers.provider_base import JobStatus, JobState

logger = logging.getLogger(__name__)


class PollItem(ExecutorStatus):
    def __init__(self, executor: ParslExecutor):
        self._executor = executor
        self._interval = executor.status_polling_interval
        self._last_poll_time = 0.0
        self._status = {}  # type: Dict[object, JobStatus]

        # Create a ZMQ channel to send poll status to monitoring
        if isinstance(self._executor, HighThroughputExecutor):
           self.monitoring_enabled = False
           hub_address = self._executor.hub_address
           hub_port = self._executor.hub_port
           if hub_address and hub_port:
               context = zmq.Context()
               self.hub_channel = context.socket(zmq.DEALER)
               self.hub_channel.set_hwm(0)
               self.hub_channel.connect("tcp://{}:{}".format(hub_address, hub_port))
               self.monitoring_enabled = True
               logger.info("Monitoring enabled on executor and connected to hub")

    def _should_poll(self, now: float):
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float):
        if self._should_poll(now):
            self._status = self._executor.status()
            self._last_poll_time = now

            # Send monitoring info for HTEX when monitoring enabled
            if isinstance(self._executor, HighThroughputExecutor):
                if self.monitoring_enabled:
                    msg = self._executor.create_monitoring_info(self._status)
                    logger.info("Sending message {} to hub from executor".format(msg))
                    self.hub_channel.send_pyobj((MessageType.BLOCK_INFO, msg)) 

    @property
    def status(self) -> Dict[object, JobStatus]:
        return self._status

    @property
    def executor(self) -> ParslExecutor:
        return self._executor

    def scale_in(self, n):
        ids = self._executor.scale_in(n)
        if ids is not None:
            for id in ids:
                del self._status[id]
        return ids

    def scale_out(self, n):
        ids = self._executor.scale_out(n)
        if ids is not None:
            for id in ids:
                self._status[id] = JobStatus(JobState.PENDING)
        return ids

    def __repr__(self):
        return self._status.__repr__()


class TaskStatusPoller(object):
    def __init__(self, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._poll_items = []  # type: List[PollItem]
        self._strategy = Strategy(dfk)
        self._error_handler = JobErrorHandler()

    def poll(self, tasks=None, kind=None):
        self._update_state()
        self._error_handler.run(self._poll_items)
        self._strategy.strategize(self._poll_items, tasks)

    def _update_state(self):
        now = time.time()
        for item in self._poll_items:
            item.poll(now)

    def add_executors(self, executors: Sequence[ParslExecutor]):
        for executor in executors:
            if executor.status_polling_interval > 0:
                logger.debug("Adding executor {}".format(executor.label))
                self._poll_items.append(PollItem(executor))
        self._strategy.add_executors(executors)
