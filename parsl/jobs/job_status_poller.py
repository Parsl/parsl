import logging
from typing import List, Optional, Sequence, Union

from parsl.executors.status_handling import BlockProviderExecutor
from parsl.jobs.strategy import Strategy
from parsl.utils import Timer

logger = logging.getLogger(__name__)


class JobStatusPoller(Timer):
    def __init__(self, *, strategy: Optional[str], max_idletime: float,
                 strategy_period: Union[float, int]) -> None:
        self._executors = []  # type: List[BlockProviderExecutor]
        self._strategy = Strategy(strategy=strategy,
                                  max_idletime=max_idletime)
        super().__init__(self.poll, interval=strategy_period, name="JobStatusPoller")

    def poll(self) -> None:
        self._update_state()
        self._run_error_handlers(self._executors)
        self._strategy.strategize(self._executors)

    def _run_error_handlers(self, executors: List[BlockProviderExecutor]) -> None:
        for e in executors:
            e.handle_errors(e.status_facade)

    def _update_state(self) -> None:
        for item in self._executors:
            item.poll_facade()

    def add_executors(self, executors: Sequence[BlockProviderExecutor]) -> None:
        for executor in executors:
            if executor.status_polling_interval > 0:
                logger.debug("Adding executor {}".format(executor.label))
                self._executors.append(executor)
        self._strategy.add_executors(executors)

    def close(self, timeout: Optional[float] = None) -> None:
        super().close(timeout)
        for executor in self._executors:
            if not executor.bad_state_is_set:
                logger.info(f"Scaling in executor {executor.label}")

                # this code needs to be at least as many blocks as need
                # cancelling, but it is safe to be more, as the scaling
                # code will cope with being asked to cancel more blocks
                # than exist.
                block_count = len(executor.status_facade)
                executor.scale_in_facade(block_count)

            else:  # and bad_state_is_set
                logger.warning(f"Not scaling in executor {executor.label} because it is in bad state")
