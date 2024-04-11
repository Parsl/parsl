import logging
import parsl
import time
from typing import Dict, List, Sequence, Optional, Union

from parsl.jobs.states import JobStatus, JobState
from parsl.jobs.strategy import Strategy
from parsl.executors.status_handling import BlockProviderExecutor


from parsl.utils import Timer


logger = logging.getLogger(__name__)


class PolledExecutorFacade:
    def __init__(self, executor: BlockProviderExecutor):
        self._executor = executor
        self._last_poll_time = 0.0
        self._status = {}  # type: Dict[str, JobStatus]

    def poll_facade(self) -> None:
        now = time.time()
        if now >= self._last_poll_time + self._executor.status_polling_interval:
            previous_status = self._status
            self._status = self._executor.status()
            self._last_poll_time = now
            delta_status = {}
            for block_id in self._status:
                if block_id not in previous_status \
                   or previous_status[block_id].state != self._status[block_id].state:
                    delta_status[block_id] = self._status[block_id]

            if delta_status:
                self._executor.send_monitoring_info(delta_status)

    @property
    def status_facade(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks of the executor of this poller.

        :return: a dictionary mapping block ids (in string) to job status
        """
        return self._status

    @property
    def executor(self) -> BlockProviderExecutor:
        return self._executor

    def scale_in_facade(self, n: int, max_idletime: Optional[float] = None) -> List[str]:

        if max_idletime is None:
            block_ids = self._executor.scale_in(n)
        else:
            # This is a HighThroughputExecutor-specific interface violation.
            # This code hopes, through pan-codebase reasoning, that this
            # scale_in method really does come from HighThroughputExecutor,
            # and so does have an extra max_idletime parameter not present
            # in the executor interface.
            block_ids = self._executor.scale_in(n, max_idletime=max_idletime)  # type: ignore[call-arg]
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.CANCELLED)
                del self._status[block_id]
            self._executor.send_monitoring_info(new_status)
        return block_ids

    def scale_out_facade(self, n: int) -> List[str]:
        block_ids = self._executor.scale_out(n)
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.PENDING)
            self._executor.send_monitoring_info(new_status)
            self._status.update(new_status)
        return block_ids


class JobStatusPoller(Timer):
    def __init__(self, *, strategy: Optional[str], max_idletime: float,
                 strategy_period: Union[float, int],
                 monitoring: Optional["parsl.monitoring.radios.MonitoringRadio"] = None) -> None:
        self._executor_facades = []  # type: List[PolledExecutorFacade]
        self._strategy = Strategy(strategy=strategy,
                                  max_idletime=max_idletime)
        super().__init__(self.poll, interval=strategy_period, name="JobStatusPoller")

    def poll(self) -> None:
        self._update_state()
        self._run_error_handlers(self._executor_facades)
        self._strategy.strategize(self._executor_facades)

    def _run_error_handlers(self, status: List[PolledExecutorFacade]) -> None:
        for es in status:
            es.executor.handle_errors(es.status_facade)

    def _update_state(self) -> None:
        for item in self._executor_facades:
            item.poll_facade()

    def add_executors(self, executors: Sequence[BlockProviderExecutor]) -> None:
        for executor in executors:
            if executor.status_polling_interval > 0:
                logger.debug("Adding executor {}".format(executor.label))
                self._executor_facades.append(PolledExecutorFacade(executor))
        self._strategy.add_executors(executors)

    def close(self, timeout: Optional[float] = None) -> None:
        super().close(timeout)
        for ef in self._executor_facades:
            if not ef.executor.bad_state_is_set:
                logger.info(f"Scaling in executor {ef.executor.label}")

                # this code needs to be at least as many blocks as need
                # cancelling, but it is safe to be more, as the scaling
                # code will cope with being asked to cancel more blocks
                # than exist.
                block_count = len(ef.status_facade)
                ef.scale_in_facade(block_count)

            else:  # and bad_state_is_set
                logger.warning(f"Not scaling in executor {ef.executor.label} because it is in bad state")
