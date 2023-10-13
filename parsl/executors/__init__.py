from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.executors.workqueue.executor import WorkQueueExecutor
    from parsl.executors.high_throughput.executor import HighThroughputExecutor
    from parsl.executors.threads import ThreadPoolExecutor
    from parsl.executors.flux.executor import FluxExecutor

lazys = {
    'ThreadPoolExecutor': 'parsl.executors.threads',
    'WorkQueueExecutor': 'parsl.executors.workqueue.executor',
    'HighThroughputExecutor': 'parsl.executors.high_throughput.executor',
    'FluxExecutor': 'parsl.executors.flux.executor',
}

import parsl.executors as px


def lazy_loader(name):
    if name in lazys:
        import importlib
        m = lazys[name]
        # print(f"lazy load {name} from module {m}")
        v = importlib.import_module(m)
        # print(f"imported module: {v}")
        a = v.__getattribute__(name)
        px.__setattr__(name, a)
        return a
    raise AttributeError(f"No (lazy loadable) attribute in {__name__} for {name}")


# parsl/executors/__init__.py:34: error: Cannot assign to a method
px.__getattr__ = lazy_loader  # type: ignore[method-assign]

__all__ = ['ThreadPoolExecutor',
           'HighThroughputExecutor',
           'WorkQueueExecutor',
           'FluxExecutor']
