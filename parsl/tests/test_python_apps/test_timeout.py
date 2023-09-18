import time

import pytest

from parsl.app.errors import AppTimeout
from parsl.app.python import timeout


class TimeoutTestSpecificException(Exception):
    pass


def kernel(fail=False):
    if fail:
        raise TimeoutTestSpecificException()


def test_timeout():
    timeout_dur = 0.1
    try:
        timeout(kernel, timeout_dur)()  # nominally returns "instantly"
    except AppTimeout:
        pytest.skip("Pre-condition failed: result not received in timely fashion")
        assert False

    try:
        time.sleep(timeout_dur + 0.01)  # this is the verification
    except AppTimeout:
        assert False, "Timer was not cancelled!"


@pytest.mark.parametrize("timeout_dur", (0.005, 0.01))
def test_timeout_after_exception(timeout_dur):
    with pytest.raises(TimeoutTestSpecificException):
        timeout(kernel, timeout_dur)(True)

    try:
        time.sleep(timeout_dur + 0.01)  # this is the verification
    except AppTimeout:
        assert False, "Timer was not cancelled!"
