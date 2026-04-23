import logging
import os
from typing import Callable

from parsl.log_utils import DEFAULT_FORMAT
from parsl.logconfigs.base import LogConfig


class FileLogging(LogConfig):

    def __init__(self, *, format_string: str = DEFAULT_FORMAT, level: int = logging.DEBUG):
        super().__init__()
        self.format_string = DEFAULT_FORMAT
        self.level = level

    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:

        os.makedirs(log_dir, exist_ok=True)
        filename = log_dir + "/" + log_name + ".log"

        logger = logging.getLogger("parsl")
        logger.setLevel(logging.DEBUG)  # TODO: max of level
        handler = logging.FileHandler(filename)
        handler.setLevel(self.level)
        formatter = logging.Formatter(self.format_string, datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # see note in set_stream_logger for notes about logging
        # concurrent.futures
        futures_logger = logging.getLogger("concurrent.futures")
        # TODO: should be a max set level here too...
        futures_logger.addHandler(handler)

        def unregister_callback():
            logger.removeHandler(handler)
            futures_logger.removeHandler(handler)

        return unregister_callback
