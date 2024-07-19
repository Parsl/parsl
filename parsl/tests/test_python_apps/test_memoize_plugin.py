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
    """Testing python memoization disable
    """

    # TODO: this .result() needs to be here, not in the loop
    # because otherwise we race to complete... and then
    # we might sometimes get a memoization before the loop
    # and sometimes not...
    x = random_uuid(0).result()

    for i in range(0, n):
        foo = random_uuid(0)
        print(i)
        print(foo.result())
        assert foo.result() == x, "Memoized results were incorrectly not used"

    y = random_uuid(7).result()

    for i in range(0, n):
        foo = random_uuid(7)
        print(i)
        print(foo.result())
        assert foo.result() != y, "Memoized results were incorrectly used"
