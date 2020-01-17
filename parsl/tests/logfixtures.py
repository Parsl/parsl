import contextlib
import logging

import pytest

logger = logging.getLogger('parsl')

got_bad_log = None
allow_bad_log = False


@pytest.fixture(autouse=True, scope='session')
def prohibit_severe_logs_session():
    logger = logging.getLogger()
    handler = ParslTestLogHandler()
    logger.addHandler(handler)


# this is done after the test has finished rather than directly in the log
# handler so as to defer the exception until somewhere that pytest will
# directly see it. If the exception happens inside the log handler, the
# upwards call stack is not necessarily going to get this properly reported
# and in some cases causes a hang (!)
@pytest.fixture(autouse=True)
def prohibit_severe_logs_test():
    global got_bad_log
    yield
    if got_bad_log is not None:
        old = got_bad_log
        got_bad_log = None
        raise ValueError("Unexpected severe log message: {}".format(old))


@contextlib.contextmanager
def permit_severe_log():
    global allow_bad_log
    old = allow_bad_log
    allow_bad_log = True
    yield
    allow_bad_log = old


class ParslTestLogHandler(logging.Handler):
    def emit(self, record):
        global got_bad_log
        if record.levelno >= 40 and not allow_bad_log:
            got_bad_log = "{}: {}".format(record.levelname, record.message)
