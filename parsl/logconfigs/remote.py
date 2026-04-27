import datetime
import logging
import os
import time
from functools import wraps
from multiprocessing import Event
from typing import Any, Callable, Dict, List, Sequence, Tuple

from parsl.logconfigs.base import LogConfig

logger = logging.getLogger(__name__)


def logging_wrapper(*,
                    f: Any,           # per app
                    args: Sequence,   # per invocation
                    kwargs: Dict,     # per invocation
                    run_id: str,      # per workflow
                    task_id: int,   # per invocation
                    try_id: int,    # per invocation
                    log_config: LogConfig,
                    run_dir: str) -> Tuple[Callable, Sequence, Dict]:
    """Wrap the Parsl app with logs according to the specified `LogConfig`.

    ``log_config`` will be initialized before the task body starts executing,
    and uninitialized when the task finishes.
    """

    def wrapped(_parsl_logging_wrapper_f, args: List[Any], **kwargs: Dict[str, Any]) -> Any:
        name = f"task_{task_id}_{try_id}" 
        cb = log_config.initialize_logging(log_dir=run_dir, log_name=name)
        try:
            return _parsl_logging_wrapper_f(*args, **kwargs)
        finally: 
            # TODO: after
            cb()

    return (wrapped, [f, args], kwargs)


