"""Parsl is a Parallel Scripting Library, designed to enable efficient workflow execution.

Importing
---------

To get all the required functionality, we suggest importing the library as follows:

>>> import parsl
>>> from parsl import *

Constants
---------
AUTO_LOGNAME
    Special value that indicates Parsl should construct a filename for logging.

"""
import logging
import os
import platform

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from parsl.executors import ThreadPoolExecutor
    from parsl.data_provider.files import File
    from parsl.dataflow.dflow import DataFlowKernel
    from parsl.app.app import bash_app, join_app, python_app
    from parsl.log_utils import set_file_logger, set_stream_logger

lazys = {
        'python_app': 'parsl.app.app',
        'bash_app': 'parsl.app.app',
        'join_app': 'parsl.app.app',
        'Config': 'parsl.config',
        'ThreadPoolExecutor': 'parsl.executors',
        'HighThroughputExecutor': 'parsl.executors',
        'ExtremeScaleExecutor': 'parsl.executors',
        'WorkQueueExecutor': 'parsl.executors',
        'set_stream_logger': 'parsl.log_utils',
        'set_file_logger': 'parsl.log_utils',
        'MonitoringHub': 'parsl.monitoring',
        'File': 'parsl.data_provider.files',
        'DataFlowKernel': 'parsl.dataflow.dflow',
        'DataFlowKernelLoader': 'parsl.dataflow.dflow',
}

import parsl


def lazy_loader(name):
    print(f"lazy_loader getattr for {name}")
    if name in lazys:
        import importlib
        m = lazys[name]
        print(f"lazy load {name} from module {m}")
        v = importlib.import_module(m)
        print(f"imported module: {v}")
        a = v.__getattribute__(name)
        parsl.__setattr__(name, a)
        return a
    raise AttributeError(f"No (lazy loadable) attribute in {__name__} for {name}")


# parsl/__init__.py:61: error: Cannot assign to a method
parsl.__getattr__ = lazy_loader  # type: ignore

import multiprocessing
if platform.system() == 'Darwin':
    multiprocessing.set_start_method('fork', force=True)


AUTO_LOGNAME = -1

# there's a reason these were aliases and not redefinitions,
# and i should fix this to keep them as such.


def clear(*args, **kwargs):
    from parsl import DataFlowKernelLoader
    return DataFlowKernelLoader.clear(*args, **kwargs)


def load(*args, **kwargs):
    from parsl import DataFlowKernelLoader
    return DataFlowKernelLoader.load(*args, **kwargs)


def dfk(*args, **kwargs):
    from parsl import DataFlowKernelLoader
    return DataFlowKernelLoader.dfk(*args, **kwargs)


def wait_for_current_tasks(*args, **kwargs):
    from parsl import DataFlowKernelLoader
    return DataFlowKernelLoader.wait_for_current_tasks(*args, **kwargs)


logging.getLogger('parsl').addHandler(logging.NullHandler())

if platform.system() == 'Darwin':
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

__author__ = 'The Parsl Team'

from parsl.version import VERSION
__version__ = VERSION

__all__ = [

    # decorators
    'bash_app',
    'python_app',
    'join_app',

    # core
    'Config',
    'DataFlowKernel',
    'File',

    # logging
    'set_stream_logger',
    'set_file_logger',
    'AUTO_LOGNAME',

    # executors
    'ThreadPoolExecutor',
    'HighThroughputExecutor',
    'ExtremeScaleExecutor',
    'WorkQueueExecutor',

    # monitoring
    'MonitoringHub',
]
