import logging
import os
from typing import Callable

from parsl.log_utils import DEFAULT_FORMAT
from parsl.logconfigs.base import LogConfig


class FileLogging(LogConfig):
    """A log configuration which configures traditional logging to files."""

    def __init__(self, *, format_string: str = DEFAULT_FORMAT, level: int = logging.DEBUG):
        super().__init__()
        self.format_string = DEFAULT_FORMAT
        self.level = level

    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:

        os.makedirs(log_dir, exist_ok=True)
        filename = log_dir + "/" + log_name + ".log"

        handler = logging.FileHandler(filename)
        handler.setLevel(self.level)
        formatter = logging.Formatter(self.format_string, datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)

        logger = logging.getLogger("parsl")

        if logger.level == logging.NOTSET:
            logger.setLevel(self.level)
        else:
            logger.setLevel(min(logger.level, self.level))

        logger.addHandler(handler)

        # Concurrent.futures errors are also of interest, as exceptions
        # which propagate out of the top of a callback are logged this way
        # and then discarded. (see #240)
        futures_logger = logging.getLogger("concurrent.futures")
        if futures_logger.level == logging.NOTSET:
            futures_logger.setLevel(self.level)
        else:
            futures_logger.setLevel(min(futures_logger.level, self.level))
        futures_logger.addHandler(handler)

        def unregister_callback():
            logger.removeHandler(handler)
            futures_logger.removeHandler(handler)

        return unregister_callback
