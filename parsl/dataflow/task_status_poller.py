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
from parsl.monitoring.message_type import MessageType

from parsl.providers.provider_base import JobStatus, JobState

logger = logging.getLogger(__name__)


class PollItem(ExecutorStatus):
    def __init__(self, executor: ParslExecutor, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._executor = executor
        self._dfk = dfk
        self._interval = executor.status_polling_interval
        self._last_poll_time = 0.0
        self._status = {}  # type: Dict[str, JobStatus]

        # Create a ZMQ channel to send poll status to monitoring
        self.monitoring_enabled = False
        if self._dfk.monitoring is not None:
            self.monitoring_enabled = True
            hub_address = self._dfk.hub_address
            hub_port = self._dfk.hub_interchange_port
            context = zmq.Context()
            self.hub_channel = context.socket(zmq.DEALER)
            self.hub_channel.set_hwm(0)
            self.hub_channel.connect("tcp://{}:{}".format(hub_address, hub_port))
            logger.info("Monitoring enabled on task status poller")

    def _should_poll(self, now: float):
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float):
        if self._should_poll(now):
            self._status = self._executor.status()
            self._last_poll_time = now
            self.send_monitoring_info(self._status)

    def send_monitoring_info(self, status=None):
        # Send monitoring info for HTEX when monitoring enabled
        if self.monitoring_enabled:
            msg = self._executor.create_monitoring_info(status)
            logger.debug("Sending message {} to hub from task status poller".format(msg))
            self.hub_channel.send_pyobj((MessageType.BLOCK_INFO, msg))

    @property
    def status(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks of the executor of this poller.

        :return: a dictionary mapping block ids (in string) to job status
        """
        return self._status

    @property
    def executor(self) -> ParslExecutor:
        return self._executor

    def scale_in(self, n, force=True, max_idletime=None):
        if force and not max_idletime:
            block_ids = self._executor.scale_in(n)
        else:
            block_ids = self._executor.scale_in(n, force=force, max_idletime=max_idletime)
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.CANCELLED)
                del self._status[block_id]
            self.send_monitoring_info(new_status)
        return block_ids

    def scale_out(self, n):
        block_ids = self._executor.scale_out(n)
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.PENDING)
            self.send_monitoring_info(new_status)
            self._status.update(new_status)
        return block_ids

    def __repr__(self):
        return self._status.__repr__()


class TaskStatusPoller(object):
    def __init__(self, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._poll_items = []  # type: List[PollItem]
        self.dfk = dfk
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
                self._poll_items.append(PollItem(executor, self.dfk))
            else:
                logger.debug("Executor {} has no poll time, so will not poll".format(executor.label))
        self._strategy.add_executors(executors)
