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
        logger.setLevel(logging.DEBUG)  # TODO: max of level
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

        """ here's whats in the parsl.log default record:
        "%(created)f %(asctime)s %(processName)s-%(process)d "
        "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s %(levelname)s: "
        "%(message)s"
        """

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

        """
        if hasattr(record, "extra"):
            d["extra"] = record.extra

        if hasattr(record, "parsl_dfk"):
            d["parsl_dfk"] = record.parsl_dfk

        if hasattr(record, "parsl_task_id"):
            d["parsl_task_id"] = record.parsl_task_id

        if hasattr(record, "parsl_try_id"):
            d["parsl_try_id"] = record.parsl_try_id
        """

        # TODO: this is a bit horrible: i want *all* the extra to be
        # logged, so that arbitrary bits of code can add arbitrary
        # interesting columns.

        d["formatted"] = self.format(record)
        d["known_keys"] = repr(record.__dict__.keys())
        for (k, v) in record.__dict__.items():
            try:
                d[k] = str(v)
            except Exception as e:
                # this is mostly for debugging
                d[k] = f"UNREPRESENTABLE: {e!r}"

        json.dump(d, fp=self.f)  # this must be on one line!
        print("", file=self.f)   # so that this newline has meaning...
        self.f.flush()  # TODO: could be via self.flush() like stream handler?
        # i was seeing the result of not-flushing in interchange.log, where
        # the interchange shuts down by being killed.
