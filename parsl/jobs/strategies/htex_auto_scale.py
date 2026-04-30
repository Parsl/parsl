from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from parsl.jobs.strategies.simple import SimpleStrategy, StrategyState

if TYPE_CHECKING:
    from parsl.executors.high_throughput.executor import HighThroughputExecutor

logger = logging.getLogger(__name__)


class HtexAutoScaleStrategy(SimpleStrategy):
    """
        HTEX specific auto scaling strategy

        This strategy works only for HTEX. This strategy will scale out by
        requesting additional compute resources via the provider when the
        workload requirements exceed the provisioned capacity. The scale out
        behavior is exactly like the 'simple' strategy.

        If there are idle blocks during execution, this strategy will terminate
        those idle blocks specifically. When # of tasks >> # of blocks, HTEX places
        tasks evenly across blocks, which makes it rather difficult to ensure that
        some blocks will reach 0% utilization. Consequently, this strategy can be
        expected to scale in effectively only when # of workers, or tasks executing
        per block is close to 1.
    """
    def __init__(self, executor: "HighThroughputExecutor", max_idletime: float) -> None:
        # This strategy only supports scaling in for HighThroughputExecutor
        super().__init__(executor, max_idletime=max_idletime)
        self.max_idletime = max_idletime

    def strategize(self) -> None:
        if self.bad_state():
            return

        self.init_blocks_once()

        state = self.get_state()

        # reset idle timer if executor has active tasks
        if state.active_tasks > 0 and self._executor_idle_since is not None:
            self._executor_idle_since = None

        # First: run all SimpleStrategy cases
        if self.no_tasks(state):
            return
        if self.needs_more_slots(state):
            return

        # Then: HTEX-specific case
        if self.more_slots_than_tasks_htex(state):
            return

        # Fallback: base case log
        self._case_base_no_change(state)

    def more_slots_than_tasks_htex(self, state: StrategyState) -> bool:
        # Case 4
        # More slots than tasks
        if not (state.active_slots > 0 and state.active_slots > state.active_tasks):
            return False

        logger.debug("%s Strategy case 4: more slots than tasks", self.build_prefix())

        # active_blocks must be above min_blocks to scale in
        if state.active_blocks <= state.min_blocks:
            return False

        excess_slots = math.floor(state.active_slots - (state.active_tasks * state.parallelism))
        excess_blocks = math.floor(float(excess_slots) / (state.tasks_per_node * state.nodes_per_block))
        excess_blocks = min(excess_blocks, state.active_blocks - state.min_blocks)

        logger.debug(
            "%s Requesting scaling in by %d blocks with idle time %.1fs",
            self.build_prefix(), excess_blocks, self.max_idletime
        )

        self.executor.scale_in_facade(excess_blocks, max_idletime=self.max_idletime)

        return True
