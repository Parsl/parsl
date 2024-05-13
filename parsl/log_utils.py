"""This module contains helpers for configuring logging. By default,
`set_file_logger` is invoked by the DataFlowKernel initializer to log
parsl messages to parsl.log.

`set_stream_logger` which by default logs to stderr, can be useful
when working in a Jupyter notebook.
"""
import io
import logging
from typing import Callable, Optional

import typeguard

DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
    "%(message)s"
)


@typeguard.typechecked
def set_stream_logger(name: str = 'parsl',
                      level: int = logging.DEBUG,
                      format_string: Optional[str] = None,
                      stream: Optional[io.TextIOBase] = None) -> Callable[[], None]:
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (string) : Set to None by default.
         - stream (io.TextIOWrapper) : Specify sys.stdout or sys.stderr for stream.
            If not specified, the default stream for logging.StreamHandler is used.
    """
    if format_string is None:
        # format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Concurrent.futures errors are also of interest, as exceptions
    # which propagate out of the top of a callback are logged this way
    # and then discarded. (see #240)
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)

    def unregister_callback():
        logger.removeHandler(handler)
        futures_logger.removeHandler(handler)

    return unregister_callback


@typeguard.typechecked
def set_file_logger(filename: str,
                    name: str = 'parsl',
                    level: int = logging.DEBUG,
                    format_string: Optional[str] = None) -> Callable[[], None]:
    """Add a file log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
        - a callable which, when invoked, will reverse the log handler
          attachments made by this call. (compare to how object based pieces
          of parsl model this as a close/shutdown/cleanup method on the
          object))
    """
    if format_string is None:
        format_string = DEFAULT_FORMAT

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # see note in set_stream_logger for notes about logging
    # concurrent.futures
    futures_logger = logging.getLogger("concurrent.futures")
    futures_logger.addHandler(handler)

    def unregister_callback():
        logger.removeHandler(handler)
        futures_logger.removeHandler(handler)

    return unregister_callback
