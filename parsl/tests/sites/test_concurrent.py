"""Tests of the interfaces to Python's concurrent library"""
from pytest import mark, raises, warns

from parsl import Config, HighThroughputExecutor, load, python_app
from parsl.concurrent import ParslPoolExecutor


def f(x):
    return x + 1


def g(x):
    return 2 * x


@python_app
def is_odd(x):
    if x % 2 == 1:
        return 1
    else:
        return 0


def make_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='test_executor',
                address="127.0.0.1",
                max_workers_per_node=2,
                heartbeat_period=2,
                heartbeat_threshold=4,
                encrypted=False,
            )
        ],
        strategy='none',
    )


@mark.local
def test_init_errors():
    with load(make_config()) as dfk, raises(ValueError, match='Specify only one of config or dfk'):
        ParslPoolExecutor(config=make_config(), dfk=dfk)

    with raises(ValueError, match='Must specify one of config or dfk'):
        ParslPoolExecutor()


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

    with raises(RuntimeError, match='shut down'):
        exc.submit(f, 1)

    with warns(UserWarning):
        ParslPoolExecutor(make_config()).shutdown(False, cancel_futures=True)


@mark.local
def test_with_dfk():
    config = make_config()

    with load(config) as dfk, ParslPoolExecutor(dfk=dfk, executors=['test_executor']) as exc:
        future = exc.submit(f, 1)
        assert future.result() == 2
        assert exc.get_app(f).executors == ['test_executor']


@mark.local
def test_chaining():
    """Make sure the executor functions can be chained together"""
    config = make_config()

    with ParslPoolExecutor(config) as exc:
        future_odd = exc.submit(f, 10)
        assert is_odd(future_odd).result()

        future_even = exc.submit(g, future_odd)
        assert not is_odd(future_even).result()
