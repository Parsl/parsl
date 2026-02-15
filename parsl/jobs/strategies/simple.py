from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from parsl.jobs.states import JobState
from parsl.jobs.strategies.none import InitOnlyStrategy

if TYPE_CHECKING:
    from parsl.executors.status_handling import BlockProviderExecutor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategyState:
    # Tasks that are either pending completion
    active_tasks: int
    status: object

    # provider/executor derived inputs
    min_blocks: int
    max_blocks: int
    tasks_per_node: int | float
    nodes_per_block: int
    parallelism: float

    # computed
    running: int
    pending: int
    active_blocks: int
    active_slots: int


class SimpleStrategy(InitOnlyStrategy):
    """Simple scaling: init_blocks once, scale out on overload, scale in only when idle."""

    def __init__(self, executor: "BlockProviderExecutor") -> None:
        super().__init__(executor)
        self._executor_idle_since: Optional[float] = None

    def strategize(self) -> None:

        if self.bad_state():
            return

        self.init_blocks_once()

        state = self.get_state()

        # reset idle timer if executor has active tasks
        if state.active_tasks > 0 and self._executor_idle_since is not None:
            self._executor_idle_since = None

        # Try each case. If one matches, stop.
        if self.no_tasks(state):
            return
        if self.overloaded_slots(state):
            return
        if self.tasks_but_no_slots(state):
            return

        # tasks ~ slots
        self._case_base_no_change(state)

    def get_state(self) -> StrategyState:
        executor = self.executor
        provider = executor.provider

        # Tasks that are either pending completion
        active_tasks = executor.outstanding()

        status = executor.status_facade

        # FIXME we need to handle case where provider does not define these
        # FIXME probably more of this logic should be moved to the provider
        min_blocks = provider.min_blocks
        max_blocks = provider.max_blocks
        tasks_per_node = executor.workers_per_node
        nodes_per_block = provider.nodes_per_block
        parallelism = provider.parallelism

        running = sum(1 for x in status.values() if x.state == JobState.RUNNING)
        pending = sum(1 for x in status.values() if x.state == JobState.PENDING)
        active_blocks = running + pending
        active_slots = active_blocks * tasks_per_node * nodes_per_block

        logger.debug(
            "%s Executor %d active tasks, %d active slots, and %d/%d running/pending blocks",
            self.build_prefix(), active_tasks, active_slots, running, pending
        )

        return StrategyState(
            active_tasks=active_tasks,
            status=status,
            min_blocks=min_blocks,
            max_blocks=max_blocks,
            tasks_per_node=tasks_per_node,
            nodes_per_block=nodes_per_block,
            parallelism=parallelism,
            running=running,
            pending=pending,
            active_blocks=active_blocks,
            active_slots=active_slots,
        )

    def no_tasks(self, state: StrategyState) -> bool:
        # Case 1
        # No tasks.
        if state.active_tasks != 0:
            return False

        # Case 1a
        # Fewer blocks that min_blocks
        if state.active_blocks <= state.min_blocks:
            logger.debug(
                "%s Strategy case 1a: Executor has no active tasks and minimum blocks. Taking no action.",
                self.build_prefix()
            )
            return True

        # Case 1b
        # More blocks than min_blocks. Scale in
        # We want to make sure that max_idletime is reached
        # before killing off resources
        logger.debug(
            "%s Strategy case 1b: Executor has no active tasks, and more (%d) than minimum blocks (%d)",
            self.build_prefix(), state.active_blocks, state.min_blocks
        )

        if self._executor_idle_since is None:
            logger.debug(
                "%s Starting idle timer. If idle time exceeds %.1fs, blocks will be scaled in",
                self.build_prefix(), self.executor.max_idletime
            )
            self._executor_idle_since = time.time()

        idle_duration = time.time() - self._executor_idle_since

        if idle_duration >= self.executor.max_idletime:
            # We have resources idle for the max duration,
            # we have to scale_in now.
            logger.debug(
                "%s Idle time has reached %.1fs for executor; scaling in",
                self.build_prefix(), self.executor.max_idletime
            )
            self.executor.scale_in_facade(state.active_blocks - state.min_blocks)
        else:
            logger.debug(
                "%s Idle time %.1fs is less than max_idletime %.1fs; not scaling in",
                self.build_prefix(), idle_duration, self.executor.max_idletime
            )

        return True

    def overloaded_slots(self, state: StrategyState) -> bool:
        # Case 2
        # More tasks than the available slots.
        if state.active_tasks <= 0:
            return False

        if not ((float(state.active_slots) / state.active_tasks) < state.parallelism):
            return False

        logger.debug(
            "%s Strategy case 2: slots are overloaded - (slot_ratio = active_slots/active_tasks) < parallelism",
            self.build_prefix()
        )

        # Case 2a
        # We have the max blocks possible
        if state.active_blocks >= state.max_blocks:
            # Ignore since we already have the max nodes
            logger.debug(
                "%s Strategy case 2a: active_blocks %d >= max_blocks %d so not scaling out",
                self.build_prefix(), state.active_blocks, state.max_blocks
            )
            return True

        # Case 2b
        logger.debug(
            "%s Strategy case 2b: active_blocks %d < max_blocks %d so scaling out",
            self.build_prefix(), state.active_blocks, state.max_blocks
        )
        excess_slots = math.ceil((state.active_tasks * state.parallelism) - state.active_slots)
        excess_blocks = math.ceil(float(excess_slots) / (state.tasks_per_node * state.nodes_per_block))
        excess_blocks = min(excess_blocks, state.max_blocks - state.active_blocks)
        logger.debug("%s Requesting %d more blocks", self.build_prefix(), excess_blocks)
        self.executor.scale_out_facade(excess_blocks)
        return True

    def tasks_but_no_slots(self, state: StrategyState) -> bool:
        # Case 3 executor
        # Tasks but no slots
        if not (state.active_slots == 0 and state.active_tasks > 0):
            return False

        logger.debug(
            "%s Strategy case 3: No active slots but some active tasks - could scale out by a single block",
            self.build_prefix()
        )

        if state.active_blocks < state.max_blocks:
            logger.debug("%s Requesting single block", self.build_prefix())
            self.executor.scale_out_facade(1)
        else:
            logger.debug("%s Not requesting any blocks, because at maxblocks already", self.build_prefix())

        return True

    def _case_base_no_change(self, state: StrategyState) -> None:
        logger.debug("%s no changes necessary to current block load", self.build_prefix())
