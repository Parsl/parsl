import logging
from typing import TYPE_CHECKING, Optional, Union

from parsl.jobs.strategies.base import ScalingStrategy
from parsl.jobs.strategies.scaling_strategy_factory import make_strategy
from parsl.utils import Timer

if TYPE_CHECKING:
    from parsl.executors.status_handling import BlockProviderExecutor

logger = logging.getLogger(__name__)


class JobStatusPoller(Timer):
    """
    This class initializes a background thread that periodically
    executes the polling loop for each BlockProviderExecutor.

    At each cycle, the poll method:

    1. Updates provider/job state.
    2. Runs executor error handlers.
    3. Applies the scaling strategy.
    """

    def __init__(
        self,
        *,
        executor: "BlockProviderExecutor",
        strategy: str,
        strategy_period: Union[float, int],
    ) -> None:
        """
        Parameters
        ----------
        executor : BlockProviderExecutor
            The executor managed by this poller.

        strategy : str
            Scaling strategy name.

        max_idletime : float
            Idle timeout used by the strategy.

        strategy_period : float | int
            Interval (in seconds) between polling cycles.
        """

        # status_polling_interval is <= 0 when the executor does not have a provider
        if executor.status_polling_interval <= 0:
            logger.warning(
                f"Could not initialize JobStatusPoller for executor {executor.label}: "
                "status_polling_interval <= 0 (no provider available)."
            )
            raise RuntimeError(
                f"Executor {executor.label} has status_polling_interval <= 0. "
                "JobStatusPoller cannot be created."
            )

        self._executor = executor

        self._strategy: ScalingStrategy = make_strategy(
            executor=executor,
            strategy=strategy
        )

        # Initialize the Timer thread with the polling method and interval
        super().__init__(
            self.poll,
            interval=strategy_period,
            name=f"JobStatusPoller-{executor.label}",
        )

        logger.info(
            f"Job status polling thread initialized for executor {executor.label}"
        )

    def poll(self) -> None:
        """
        Main polling loop executed by the Timer thread.
        """

        # Update provider state and collect executor errors
        self._executor.poll_facade()
        self._executor.handle_errors(self._executor.status_facade)

        # Run scaling strategy
        self._strategy.strategize()

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Stop polling thread and scale in remaining blocks.
        """
        super().close(timeout)

        if not self._executor.bad_state_is_set:
            logger.info(f"Scaling in executor {self._executor.label}")

            # this code needs to be at least as many blocks as need
            # cancelling, but it is safe to be more, as the scaling
            # code will cope with being asked to cancel more blocks
            # than exist.
            block_count = len(self._executor.status_facade)
            self._executor.scale_in_facade(block_count)

        else:
            logger.warning(
                f"Not scaling in executor {self._executor.label} because it is in bad state"
            )
