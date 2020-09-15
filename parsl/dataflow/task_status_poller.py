import logging
import parsl  # noqa F401 (used in string type annotation)
import time
from typing import Dict, Sequence
from typing import List  # noqa F401 (used in type annotation)

from parsl.dataflow.executor_status import ExecutorStatus
from parsl.dataflow.job_error_handler import JobErrorHandler
from parsl.dataflow.strategy import Strategy
from parsl.executors.base import ParslExecutor
from parsl.providers.provider_base import JobStatus, JobState

logger = logging.getLogger(__name__)


class PollItem(ExecutorStatus):
    def __init__(self, executor: ParslExecutor):
        self._executor = executor
        self._interval = executor.status_polling_interval
        self._last_poll_time = 0.0
        self._status = {}  # type: Dict[object, JobStatus]

    def _should_poll(self, now: float):
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float):
        if self._should_poll(now):
            self._status = self._executor.status()
            self._last_poll_time = now

    @property
    def status(self) -> Dict[object, JobStatus]:
        return self._status

    @property
    def executor(self) -> ParslExecutor:
        return self._executor

    def scale_in(self, n, force=True, max_idletime=None):
        if force and not max_idletime:
            ids = self._executor.scale_in(n)
        else:
            ids = self._executor.scale_in(n, force=force, max_idletime=max_idletime)
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
