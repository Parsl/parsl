from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.executors.status_handling import BlockProviderExecutor


class ScalingStrategy(ABC):

    def __init__(self, executor: "BlockProviderExecutor") -> None:
        self.executor = executor

    @abstractmethod
    def strategize(self) -> None:
        """Run one strategy cycle for the bound executor."""
        raise NotImplementedError
