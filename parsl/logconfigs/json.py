import json
import logging
import pathlib
from typing import Any, Callable

from parsl.logconfigs.base import LogConfig


class JSONLogging(LogConfig):
    """A log configuration which logs as JSON objects.

    This configuration will write each log event as a JSON object into a
    log file, one per line.
    """

    def __init__(self, *, level: int = logging.DEBUG):
        super().__init__()
        self.level = level

    def initialize_logging(self, *, log_dir: pathlib.Path, log_name: str) -> Callable[[], None]:
        log_dir.mkdir(exist_ok=True)

        filename = log_dir.joinpath(log_name + ".jsonlog")

        logger = logging.getLogger("")
        if logger.level == logging.NOTSET:
            logger.setLevel(self.level)
        else:
            logger.setLevel(min(logger.level, self.level))

        handler = JSONHandler(filename)
        handler.setLevel(self.level)
        formatter = logging.Formatter("%(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        def unregister_callback() -> None:
            logger.removeHandler(handler)

        return unregister_callback


class JSONHandler(logging.Handler):
    def __init__(self, filename: pathlib.Path, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.f = open(filename, "w")

    def emit(self, record: logging.LogRecord) -> None:

        d: dict[str, object] = {
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
            "formatted": self.format(record)
        }

        for (k, v) in record.__dict__.items():
            if k.startswith("parsl."):
                try:
                    d[k] = str(v)
                except Exception as e:
                    # present the exception rather than omitting
                    d[k] = f"UNREPRESENTABLE: {e!r}"

        json.dump(d, fp=self.f)
        print("", file=self.f)
        self.f.flush()
