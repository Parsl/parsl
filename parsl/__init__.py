'''
Parsl
=====

Parallel Scripting Library, designed to enable efficient workflow execution.

Importing
---------

To get all the required functionality, we suggest importing the library as follows:

>>> import parsl
>>> from parsl import *

Logging
-------

Following the general logging philosophy of python libraries, by default
`Parsl <https://github.com/swift-lang/swift-e-lab/>`_ doesn't log anything.
However the following helper functions are provided for logging:

1. set_stream_logger
    This sets the logger to the StreamHandler. This is quite useful when working from
    a Jupyter notebook.

2. set_file_logger
    This sets the logging to a file. This is ideal for reporting issues to the dev team.

'''
import logging

from parsl.version import VERSION
from parsl.app.app import App
from parsl.executors.threads import ThreadPoolExecutor
from parsl.executors.ipp import IPyParallelExecutor
from parsl.data_provider.files import File
import parsl.execution_provider

from parsl.dataflow.dflow import DataFlowKernel
from parsl.app.app_factory import AppFactoryFactory
APP_FACTORY_FACTORY = AppFactoryFactory('central')
#print(APP_FACTORY)

__author__ = 'Yadu Nand Babuji'
__version__ = VERSION

__all__ = ['App', 'DataFlowKernel', 'ThreadPoolExecutor', 'IPyParallelExecutor']

def set_stream_logger(name='parsl', level=logging.DEBUG, format_string=None):
    '''
    Add a stream log handler

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (sting) : Set to None by default.

    Returns:
         - None
    '''

    if format_string is None:
        #format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        format_string = "%(asctime)s %(name)s [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_file_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NullHandler(logging.Handler):
    ''' Setup default logging to /dev/null since this is library.

    '''

    def emit(self, record):
        pass


logging.getLogger('parsl').addHandler(NullHandler())
