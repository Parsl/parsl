from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from parsl.jobs.strategies.simple import SimpleStrategy, StrategyState

if TYPE_CHECKING:
    from parsl.executors.high_throughput.executor import HighThroughputExecutor

logger = logging.getLogger(__name__)


class HtexAutoScaleStrategy(SimpleStrategy):
    """Like SimpleStrategy, but also scales in when capacity is clearly excess.
    """
    def __init__(self, executor: "HighThroughputExecutor") -> None:
        # This strategy only supports scaling in for HighThroughputExecutor
        super().__init__(executor)

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
        if self.overloaded_slots(state):
            return
        if self.tasks_but_no_slots(state):
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
            self.build_prefix(), excess_blocks, self.executor.max_idletime
        )

        self.executor.scale_in_facade(excess_blocks)

        return True
