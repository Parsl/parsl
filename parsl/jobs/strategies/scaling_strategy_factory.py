from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.executors.status_handling import BlockProviderExecutor

from parsl.jobs.strategies.base import ScalingStrategy
from parsl.jobs.strategies.htex_auto_scale import HtexAutoScaleStrategy
from parsl.jobs.strategies.none import InitOnlyStrategy
from parsl.jobs.strategies.simple import SimpleStrategy


def make_strategy(
    *,
    executor: "BlockProviderExecutor",
    strategy: str,
) -> ScalingStrategy:
    """Factory de strategies executor-scoped.

    Retorna uma strategy nova para este executor (não reutiliza instância).
    """
    key = strategy

    if key in ("none"):
        return InitOnlyStrategy(executor)

    if key == "simple":
        return SimpleStrategy(executor)

    if strategy == "htex_auto_scale":
        from parsl.executors.high_throughput.executor import HighThroughputExecutor
        if not isinstance(executor, HighThroughputExecutor):
            raise TypeError("htex_auto_scale strategy requires HighThroughputExecutor")
        return HtexAutoScaleStrategy(executor)

    raise ValueError(f"Unknown scaling strategy: {strategy!r}")
