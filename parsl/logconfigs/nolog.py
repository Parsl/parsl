from typing import Callable

from parsl.logconfigs.base import LogConfig


class NoLogging(LogConfig):

    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:

        def unregister_callback():
            pass

        return unregister_callback
