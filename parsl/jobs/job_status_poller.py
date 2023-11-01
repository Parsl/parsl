import logging
import parsl  # noqa F401 (used in string type annotation)
import time
import zmq
from typing import Dict, Sequence
from typing import List  # noqa F401 (used in type annotation)

from parsl.jobs.states import JobStatus, JobState
from parsl.jobs.strategy import Strategy
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.monitoring.message_type import MessageType


from parsl.utils import Timer


logger = logging.getLogger(__name__)


class PollItem:
    def __init__(self, executor: BlockProviderExecutor, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
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
            logger.info("Monitoring enabled on job status poller")

    def _should_poll(self, now: float) -> bool:
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float) -> None:
        if self._should_poll(now):
            previous_status = self._status
            self._status = self._executor.status()
            self._last_poll_time = now
            delta_status = {}
            for block_id in self._status:
                if block_id not in previous_status \
                   or previous_status[block_id].state != self._status[block_id].state:
                    delta_status[block_id] = self._status[block_id]

            if delta_status:
                self.send_monitoring_info(delta_status)

    def send_monitoring_info(self, status: Dict) -> None:
        # Send monitoring info for HTEX when monitoring enabled
        if self.monitoring_enabled:
            msg = self._executor.create_monitoring_info(status)
            logger.debug("Sending message {} to hub from job status poller".format(msg))
            self.hub_channel.send_pyobj((MessageType.BLOCK_INFO, msg))

    @property
    def status(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks of the executor of this poller.

        :return: a dictionary mapping block ids (in string) to job status
        """
        return self._status

    @property
    def executor(self) -> BlockProviderExecutor:
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

    def __repr__(self) -> str:
        return self._status.__repr__()


class JobStatusPoller(Timer):
    def __init__(self, dfk: "parsl.dataflow.dflow.DataFlowKernel") -> None:
        self._poll_items = []  # type: List[PollItem]
        self.dfk = dfk
        self._strategy = Strategy(strategy=dfk.config.strategy,
                                  max_idletime=dfk.config.max_idletime)
        super().__init__(self.poll, interval=5, name="JobStatusPoller")

    def poll(self) -> None:
        self._update_state()
        self._run_error_handlers(self._poll_items)
        self._strategy.strategize(self._poll_items)

    def _run_error_handlers(self, status: List[PollItem]) -> None:
        for es in status:
            es.executor.handle_errors(es.status)

    def _update_state(self) -> None:
        now = time.time()
        for item in self._poll_items:
            item.poll(now)

    def add_executors(self, executors: Sequence[BlockProviderExecutor]) -> None:
        for executor in executors:
            if executor.status_polling_interval > 0:
                logger.debug("Adding executor {}".format(executor.label))
                self._poll_items.append(PollItem(executor, self.dfk))
        self._strategy.add_executors(executors)
