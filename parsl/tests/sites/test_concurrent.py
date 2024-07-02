"""Tests of the interfaces to Python's concurrent library"""
from pytest import mark, warns

from parsl import Config, HighThroughputExecutor
from parsl.concurrent import ParslPoolExecutor


def f(x):
    return x + 1


def make_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                address="127.0.0.1",
                max_workers_per_node=2,
                heartbeat_period=2,
                heartbeat_threshold=4,
                encrypted=True,
            )
        ],
        strategy='none',
    )


@mark.local
def test_executor():
    my_config = make_config()

    with ParslPoolExecutor(my_config) as exc:
        # Test a single submit
        future = exc.submit(f, 1)
        assert future.result() == 2

        # Make sure the map works
        results = list(exc.map(f, [1, 2, 3]))
        assert results == [2, 3, 4]

        # Make sure map works with a timeout
        results = list(exc.map(f, [1, 2, 3], timeout=5))
        assert results == [2, 3, 4]

        # Make sure only one function was registered
        assert exc.app_count == 1

    with warns(UserWarning):
        ParslPoolExecutor(make_config()).shutdown(False, cancel_futures=True)
