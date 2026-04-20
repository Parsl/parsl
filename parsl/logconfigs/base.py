from __future__ import annotations

import abc
import uuid
from collections.abc import Callable


class LogConfig(abc.ABC):
    """Implementations of this class can initialize Parsl logging.

    Parsl logging is built around Python's `logging` system, with
    an Parsl-provided configuration system that allows multiple
    configurations to be initialized and deinitialized within the
    same process.

    Configurations which might be shared between processes should expect to
    be pickled/unpickled multiple times as they move around a Parsl
    distributed system.

    A log configuration is identified across the distributed system by
    the `uuid` attribute. This allows the `log_context` context manager to
    only initialize a particular configuration once per process, and only
    deinitialize it when all users of it are finished with it.
    """

    def __init__(self) -> None:
        self.uuid = str(uuid.uuid4())

    @abc.abstractmethod
    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:
        """Initialize logging in current process.

        This should not be called by end users. Instead, users should use
        `with log_context(config)` to ensure that the configuration is
        initialized and deinitialized properly when there are overlapping
        uses of the same configuration.

        This should return a callback to uninitialize the logging initialized
        by this call. `log_context` will call that callback when appropriate.
        """
        ...
