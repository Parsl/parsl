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

from parsl.version import VERSION
from parsl.app.app import bash_app, python_app
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from parsl.executors import HighThroughputExecutor
from parsl.executors import ExtremeScaleExecutor
from parsl.executors import WorkQueueExecutor
from parsl.log_utils import set_stream_logger
from parsl.log_utils import set_file_logger
from parsl.monitoring import MonitoringHub

from parsl.data_provider.files import File

from parsl.dataflow.dflow import DataFlowKernel, DataFlowKernelLoader

__author__ = 'The Parsl Team'
__version__ = VERSION

AUTO_LOGNAME = -1

__all__ = [

    # decorators
    'bash_app',
    'python_app',

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

clear = DataFlowKernelLoader.clear
load = DataFlowKernelLoader.load
dfk = DataFlowKernelLoader.dfk
wait_for_current_tasks = DataFlowKernelLoader.wait_for_current_tasks


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


logging.getLogger('parsl').addHandler(NullHandler())

if platform.system() == 'Darwin':
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
