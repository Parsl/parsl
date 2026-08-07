import pathlib
from typing import Callable

from parsl.logconfigs.base import LogConfig


class NoopLogging(LogConfig):
    """A log configuration which doesn't configure any logging.

    This is intended to help with benchmarking, but can also be used to
    configure an environment with no logging from any LogConfig aware
    component.
    """

    def initialize_logging(self, *, log_dir: pathlib.Path, log_name: str) -> Callable[[], None]:

        def unregister_callback() -> None:
            pass

        return unregister_callback
