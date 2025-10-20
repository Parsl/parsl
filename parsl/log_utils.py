"""This module contains helpers for configuring logging. By default,
`set_file_logger` is invoked by the DataFlowKernel initializer to log
parsl messages to parsl.log.

`set_stream_logger` which by default logs to stderr, can be useful
when working in a Jupyter notebook.
"""
import io
import json
import logging
import os
from typing import Callable, Optional

import typeguard

logger = logging.getLogger(__name__)

# the (singleton) callback that should be invoked at the start of every new process
# to set up logging consistent with the parent.
_parsl_process_loginit: Callable | None = None


DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
    "%(message)s"
)

JSON_DEFAULT_FORMAT = (
    "%(message)s"
    # everything else is removed so that we end up with the message with
    # substitutions but without other metadata inlaid.
    # that makes the formatted message more useful/readable
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


class JSONHandler(logging.Handler):
    def __init__(self, filename, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.f = open(filename, "w")

    def emit(self, record):

        """ here's whats in the parsl.log default record:
        "%(created)f %(asctime)s %(processName)s-%(process)d "
        "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
        "%(message)s"
        """

        d = {
            "created": record.created,
            "process": record.process,
            "processName": record.processName,
            "threadName": record.threadName,
            "thread": record.thread,
            "name": record.name,
            "lineno": record.lineno,
            "funcname": record.funcName,
            "levelname": record.levelname,
            "msg": record.msg,
            "repr": repr(record),
        }

        """
        if hasattr(record, "extra"):
            d["extra"] = record.extra

        if hasattr(record, "parsl_dfk"):
            d["parsl_dfk"] = record.parsl_dfk

        if hasattr(record, "parsl_task_id"):
            d["parsl_task_id"] = record.parsl_task_id

        if hasattr(record, "parsl_try_id"):
            d["parsl_try_id"] = record.parsl_try_id
        """

        # TODO: this is a bit horrible: i want *all* the extra to be
        # logged, so that arbitrary bits of code can add arbitrary
        # interesting columns.

        d["formatted"] = self.format(record)
        d["known_keys"] = repr(record.__dict__.keys())
        for (k, v) in record.__dict__.items():
            try:
                d[k] = str(v)
            except Exception as e:
                # this is mostly for debugging
                d[k] = f"UNREPRESENTABLE: {e!r}"

        json.dump(d, fp=self.f)  # this must be on one line!
        print("", file=self.f)   # so that this newline has meaning...
        self.f.flush()  # TODO: could be via self.flush() like stream handler?
        # i was seeing the result of not-flushing in interchange.log, where
        # the interchange shuts down by being killed.


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
    handler = logging.FileHandler(filename=filename)
    # handler = JSONHandler(filename=filename)
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


@typeguard.typechecked
def set_json_file_logger(filename: str,
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
        format_string = JSON_DEFAULT_FORMAT

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # handler = logging.FileHandler(filename=filename)
    handler = JSONHandler(filename=filename)
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


def initialize_cross_process_logs(rundir, logname):
    print(f"BENC: in cross process log init, logname {logname}")
    if _parsl_process_loginit is not None:
        _parsl_process_loginit(rundir, logname)
        logger.debug("Initialized cross-process logging for %s in %s", logname, rundir)
    else:
        # traditional parsl logging
        # TODO: feed in legacy configuration options
        os.makedirs(rundir, exist_ok=True)
        set_file_logger(f"{rundir}/{logname}.log", level=logging.DEBUG, name='')


class LexicalSpan:
    """A context manager for observing lexically scoped spans.

    questions/notes:

    1. should there be a uuid-style opaque identifier generated here?
    probably yes.

    2. how does this relate to enclosing spans?

    3. using `with` syntax leads to more indentation. how does that affect
    readability?
    """

    def __init__(self, logger: logging.Logger | logging.LoggerAdapter, description: str):
        """description is human readable.
        """
        self.description = description
        self._logger = logger

    def __enter__(self):
        self._logger.debug(f"{self.description}: start", stacklevel=2)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is None:
            self._logger.debug(f"{self.description}: end", stacklevel=2)
        else:
            self._logger.debug(f"{self.description}: end with exception", stacklevel=2)
            # the exception could be logged here or not. and other
            # observability channels that store more structured stuff
            # might like that - even if parsl.log doesn't like it.
