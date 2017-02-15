''' Parsl

Parallel Scripting Library, designed to enable efficient workflow execution.
'''

from parsl.app.app import APP, App
from parsl.app.executors import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import parsl.app.errors
from parsl.dataflow.dflow import DataFlowKernel

__author__  = 'Yadu Nand Babuji'
__version__ = '0.1.0'

__all__ = ['App', 'DataFlowKernel', 'ThreadPoolExecutor', 'ProcessPoolExecutor']

def set_stream_logger(name='parsl', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_file_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler
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


class NullHandler (logging.Handler):
    ''' Setup default logging to /dev/null since this is library.
    '''
    def emit(self, record):
        pass

logging.getLogger('parsl').addHandler(NullHandler())
