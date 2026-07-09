import logging
from typing import List, Optional, Sequence, Union

from parsl.executors.status_handling import BlockProviderExecutor
from parsl.jobs.strategy import Strategy
from parsl.utils import Timer

logger = logging.getLogger(__name__)


class JobStatusPoller(Timer):

    def __init__(self, *, strategy: Optional[str], max_idletime: float,
                 strategy_period: Union[float, int]) -> None:
        """Initalize the poller.

        strategy: str
          names the scaling strategy to use

        max_idletime: float, seconds
          used by some scaling strategies to decide how long a block is idle

        strategy_period: float|int, seconds
          how often the scaling strategy should be executed
        """
        # only executors which should be polled and which have a valid
        # provider should be added to this list: so it is safe to assume
        # that e.provider is not None for e in self._executors
        self._executors: List[BlockProviderExecutor] = []

        self._strategy = Strategy(strategy=strategy,
                                  max_idletime=max_idletime)
        super().__init__(self.poll, interval=strategy_period, name="JobStatusPoller")

    def poll(self) -> None:
        for executor in self._executors:
            # Update status
            executor.poll_facade()
            executor.handle_errors(executor.status_facade)

        self._strategy.strategize(self._executors)

    def add_executors(self, executors: Sequence[BlockProviderExecutor]) -> None:
        for executor in executors:
            if executor.status_polling_interval > 0:
                assert executor.provider is not None, \
                    "BlockProviderExecutor.status_polling_interval implementation guarantee: None => status_polling_interval == 0"
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
