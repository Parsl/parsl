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
    the `uuid` attribute. This is intended to allow code calling the
    log_configuration class to only initialise a particular log configuration
    once per process.
    """

    def __init__(self) -> None:
        self.uuid = str(uuid.uuid4())

    @abc.abstractmethod
    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:
        """Initialize logging in current process.

        This should be implemented by users wanting to define their own log
        configuration policies.

        This should be called by Parsl components which which to initialize
        logging according to a user supplied policy, rather than a hard-coded
        log policy.

        This should return a callback to uninitialize the logging initialized
        by this call.
        """
        ...
