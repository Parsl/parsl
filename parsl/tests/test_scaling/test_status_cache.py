import parsl
import pytest
import time

from parsl.providers import LocalProvider
from parsl.tests.configs.htex_local import fresh_config

T = 0.25  # time constant to adjust timings throughout this test, seconds

CACHE_PERIOD = T*3


class TestProvider(LocalProvider):

    def __init__(self):
        self.count = 0
        super().__init__(init_blocks=1, min_blocks=1, max_blocks=1)

    @property
    def status_polling_interval(self):
        return CACHE_PERIOD

    def status(self, *args, **kwargs):
        self.count += 1
        return super().status(*args, **kwargs)


def local_setup():
    config = fresh_config()  # TODO: explicit parsl config here rather than so many modifications
    config.strategy = 'simple'
    config.strategy_period = T
    config.executors[0].poll_period = 1
    config.executors[0].max_workers_per_node = 1
    config.executors[0]._provider = TestProvider()

    dfk = parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup()
    parsl.clear()


@parsl.python_app
def noop():
    pass


# test a few ways:
@pytest.mark.local
def test_cache():

    # This is how many times the cache period we will wait
    # for scaling/provider caching activity to happen.
    # It doesn't matter too much what this number is as long
    # as its an integer bigger than one. Later in the test,
    # we'll count that cache refresh from the provider only
    # happened K-ish times.
    K = 4

    provider = parsl.dfk().config.executors[0].provider

    c1 = provider.count

    time.sleep(CACHE_PERIOD*K)

    c2 = provider.count

    # check that the provider was refreshed either K or K-1
    # times - it might be K-1 because over overlap/non-alignment
    # of the above time.sleep vs polling periods.
    assert c2 - c1 <= K, "Provider status was requested too many times"
    assert c2 - c1 >= K-1, "Provider status was requested too few times"
