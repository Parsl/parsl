import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.dataflow.taskrecord import TaskRecord


class DontReuseSevenMemoizer(BasicMemoizer):
    def check_memo(self, task_record: TaskRecord):
        if task_record['args'][0] == 7:
            return None  # we didn't find a suitable memo record...
        else:
            return super().check_memo(task_record)


def local_config():
    return Config(memoizer=DontReuseSevenMemoizer())


@python_app(cache=True)
def random_uuid(x, cache=True):
    import uuid
    return str(uuid.uuid4())


@pytest.mark.local
def test_python_memoization(n=10):
    """Testing that the plugged-in memoizer is used and behaves as expected.

    Memoizations of invocations with parameter 0 should behave like BasicMemoizer,
    but memoizations of invocations with parameter 7 should never be used, because
    the plugged-in DontReuseSevenMemoizer has a policy encoded to ignore such
    invocations.
    """
    x = random_uuid(0).result()

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() == x, "Memoized results were incorrectly not used"

    y = random_uuid(7).result()

    for i in range(0, n):
        foo = random_uuid(7)
        assert foo.result() != y, "Memoized results were incorrectly used"
