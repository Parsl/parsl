from __future__ import annotations

import abc
import logging
import pathlib
import threading
import uuid
from collections.abc import Callable

logger = logging.getLogger(__name__)


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

    def __init__(self) -> None:
        self.uuid = uuid.uuid4()

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


class _ConfigReference:
    def __init__(self, uninit_callback: Callable) -> None:
        self.uninit_callback = uninit_callback
        self.count: int = 1


# This holds the collection of initialized configurations for the current
# Python interpreter. It is scoped at the interpreter level because that is
# the scope of Python `logging` module log configuration.
_initialized_log_contexts: dict[uuid.UUID, _ConfigReference] = {}
_initialized_log_context_lock: threading.Lock = threading.Lock()


def oneshot_initialize_logging(*, log_config: LogConfig, log_dir: pathlib.Path, log_name: str) -> Callable:
    """Initialized the LogConfig if it is not currently initialized.

    This is scoped per-interpreter, the same as the global state for the
    Python logging module.

    A first oneshot_initialize_logging for any given LogConfig will call
    the initialize_logging method of that config. Subsequent calls will
    be reference counted, but not cause a reinitialization.

    Calling the callback will decrease the reference count, and when that
    count reaches zero, the LogConfig will be uninitialized via its own
    callback.

    The API syntax mimics the syntax of the LogConfig abstract class, with
    these reference counting semantics added on top.

    The intention is to allow a log configuration to be initialized by
    multiple pieces of Parsl without duplicating logs: for example, when
    using the thread pool executor, a task does not need the global log
    configuration to be initialized (again) because the task runs in the
    same process as the DFK; but in, for example, the Work Queue executor
    (at least now), there is no per-worker log configuration, and the task
    *does* need to initialize logging. This reference counting is the
    mechanism by which that different behaviour can occur.

    There is an ambiguity on the use of log_name and log_dir across multiple
    initializations: the name and directory of the first initialization
    will be used and the name and directory of subsequent initializations
    will be ignored.
    """
    with _initialized_log_context_lock:
        if log_config.uuid in _initialized_log_contexts:
            _initialized_log_contexts[log_config.uuid].count += 1
            logger.debug('Configuration %r already initialized, now with %s references',
                         log_config,
                         _initialized_log_contexts[log_config.uuid].count)
        else:
            logger.debug('Initializing %r', log_config)
            _initialized_log_contexts[log_config.uuid] = _ConfigReference(
                log_config.initialize_logging(log_dir=log_dir, log_name=log_name)
            )
            assert callable(_initialized_log_contexts[log_config.uuid].uninit_callback), (
                f'Log config {log_config} should have returned a callable on initialization'
            )
            logger.debug('Initialized %r', log_config)

        assert log_config.uuid in _initialized_log_contexts

    def uninit() -> None:
        with _initialized_log_context_lock:
            assert log_config.uuid in _initialized_log_contexts, f"log context for {log_config} not found, perhaps too many uninitializations"
            assert _initialized_log_contexts[log_config.uuid].count >= 1, \
                f"log context for {log_config} has bad reference count {_initialized_log_contexts[log_config.uuid].count}"
            _initialized_log_contexts[log_config.uuid].count -= 1
            if _initialized_log_contexts[log_config.uuid].count < 1:
                logger.debug('All references removed for %r', log_config)
                _initialized_log_contexts[log_config.uuid].uninit_callback()
                del _initialized_log_contexts[log_config.uuid]

    return uninit
