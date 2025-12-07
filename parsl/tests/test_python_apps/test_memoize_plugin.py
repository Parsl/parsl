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
@pytest.mark.parametrize("n", (10,))
def test_python_memoization(n: int):
    """Testing using an alternate memoization plugin.

    The plugin under test memoizes except for functions with the parameter 7,
    and that behaviour should be observable in uuid app results.
    """

    x = random_uuid(0).result()

    for i in range(0, n):
        foo = random_uuid(0)
        assert foo.result() == x, "Memoized results were incorrectly not used"

    y = random_uuid(7).result()

    for i in range(0, n):
        foo = random_uuid(7)
        assert foo.result() != y, "Memoized results were incorrectly used"
