from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from parsl.jobs.strategies.base import ScalingStrategy

if TYPE_CHECKING:
    from parsl.executors.status_handling import BlockProviderExecutor

logger = logging.getLogger(__name__)


class InitOnlyStrategy(ScalingStrategy):
    """Scale out init_blocks at the start, then nothing more."""

    def __init__(self, executor: "BlockProviderExecutor") -> None:
        super().__init__(executor)
        self._first = True

    def build_prefix(self) -> str:
        return f"[Scaling executor {self.executor.label}]"

    def strategize(self) -> None:
        if self.bad_state():
            return

        self.init_blocks_once()

    def init_blocks_once(self) -> None:
        executor = self.executor
        provider = executor.provider
        if provider is None:
            return

        if self._first:
            logger.debug("%s Scaling out %d initial blocks", self.build_prefix(), provider.init_blocks)
            executor.scale_out_facade(provider.init_blocks)
            self._first = False

    def bad_state(self) -> bool:
        executor = self.executor

        if executor.bad_state_is_set:
            logger.info("%s Not strategizing for executor because bad state is set", self.build_prefix())
            return True

        logger.debug("%s Strategizing for executor", self.build_prefix())

        return False
