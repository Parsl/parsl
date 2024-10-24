import threading

import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

# Timing notes:
# The configured strategy_period must be much smaller than the delay in
# app() so that multiple iterations of the strategy have had a chance
# to (mis)behave.
# The status polling interval in OneShotLocalProvider must be much bigger
# than the above times, so that the job status cached from the provider
# will not be updated while the single invocation of app() runs.


@parsl.python_app
def app():
    import time
    time.sleep(1)


class OneShotLocalProvider(LocalProvider):
    def __init__(self, *args, **kwargs):
        self.recorded_submits = 0
        super().__init__(*args, **kwargs)

    def submit(self, *args, **kwargs):
        self.recorded_submits += 1
        return super().submit(*args, **kwargs)

    status_polling_interval = 600


@pytest.mark.local
def test_one_block(tmpd_cwd):
    """
    this test is intended to ensure that only one block is launched when only
    one app is invoked. this is a regression test.
    """
    oneshot_provider = OneShotLocalProvider(
        init_blocks=0,
        min_blocks=0,
        max_blocks=10,
        launcher=SimpleLauncher(),
    )

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                address="127.0.0.1",
                worker_debug=True,
                cores_per_worker=1,
                encrypted=True,
                provider=oneshot_provider,
                worker_logdir_root=str(tmpd_cwd)
            )
        ],
        strategy='simple',
        strategy_period=0.1
    )

    with parsl.load(config):
        app().result()

    assert oneshot_provider.recorded_submits == 1
