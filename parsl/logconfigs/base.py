from __future__ import annotations

import abc
import pathlib
from collections.abc import Callable


class LogConfig(abc.ABC):
    """Implementations of this class can initialize Parsl logging.

    Parsl logging is built around Python's `logging` system, with
    a Parsl-provided configuration system that allows multiple
    configurations to be initialized and deinitialized within the
    same process.

    Configurations which might be shared between processes should expect to
    be pickled/unpickled multiple times as they move around a Parsl
    distributed system.
    """

    @abc.abstractmethod
    def initialize_logging(self, *, log_dir: pathlib.Path, log_name: str) -> Callable[[], None]:
        """Initialize logging in current process.

        This should be implemented by users wanting to define their own log
        configuration policies.

        This should be called by Parsl components to initialize logging
        according to a user supplied policy, rather than a hard-coded log
        policy.

        This should return a callback to uninitialize the logging initialized
        by this call.
        """
        ...
