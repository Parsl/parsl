import json
import logging
import os
from typing import Callable

from parsl.logconfigs.base import LogConfig


class JSONLogging(LogConfig):

    def __init__(self, *, level: int = logging.DEBUG):
        super().__init__()
        self.level = level

    def initialize_logging(self, *, log_dir: str, log_name: str) -> Callable[[], None]:
        os.makedirs(log_dir, exist_ok=True)

        filename = log_dir + "/" + log_name + ".jsonlog"

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

        def unregister_callback():
            logger.removeHandler(handler)

        return unregister_callback


class JSONHandler(logging.Handler):
    def __init__(self, filename, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.f = open(filename, "w")

    def emit(self, record):

        d = {
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
            "repr": repr(record),
        }

        d["formatted"] = self.format(record)
        d["known_keys"] = repr(record.__dict__.keys())
        for (k, v) in record.__dict__.items():
            try:
                d[k] = str(v)
            except Exception as e:
                # this is mostly for debugging
                d[k] = f"UNREPRESENTABLE: {e!r}"

        json.dump(d, fp=self.f)
        print("", file=self.f)
        self.f.flush()
