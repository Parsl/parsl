"""Following the general logging philosophy of python libraries, by default
Parsl doesn't log anything.  However the following helper functions are
provided for logging:

1. set_stream_logger
    This sets the logger to the StreamHandler. This is quite useful when working from
    a Jupyter notebook.

2. set_file_logger
    This sets the logging to a file. This is ideal for reporting issues to the dev team.

"""
import io
import logging
import typeguard

from typing import Optional


DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
    "%(message)s"
)


@typeguard.typechecked
def set_stream_logger(name: str = 'parsl',
                      level: int = logging.DEBUG,
                      format_string: Optional[str] = None,
                      stream: Optional[io.TextIOWrapper] = None) -> None:
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (string) : Set to None by default.
         - stream (io.TextIOWrapper) : Specify sys.stdout or sys.stderr for stream.
            If not specified, the default stream for logging.StreamHandler is used.

    Returns:
         - None
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


@typeguard.typechecked
def set_file_logger(filename: str,
                    name: str = 'parsl',
                    level: int = logging.DEBUG,
                    format_string: Optional[str] = None) -> None:
    """Add a file log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
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
