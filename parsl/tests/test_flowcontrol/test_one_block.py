# this test is intended to ensure that only one block is launched when only
# one app is invoked. this is a regression test.

import logging
import parsl
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
import pytest


logger = logging.getLogger(__name__)


@parsl.python_app
def app():
    import time
    time.sleep(45)


class OneShotLocalProvider(LocalProvider):
    def __init__(self, *args, **kwargs):
        logger.info("OneShotLocalProvider __init__ with MRO: {}".format(type(self).mro()))
        self.recorded_submits = 0
        super().__init__(*args, **kwargs)

    def submit(self, *args, **kwargs):
        logger.info("OneShotLocalProvider submit")
        self.recorded_submits += 1
        return super().submit(*args, **kwargs)

    status_polling_interval = 600


@pytest.mark.local
def test_one_block():

    oneshot_provider = OneShotLocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=10,
                    launcher=SimpleLauncher(),
                )

    config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                worker_debug=True,
                cores_per_worker=1,
                provider=oneshot_provider,
            )
        ],
        strategy='simple',
    )

    parsl.load(config)

    f = app()
    f.result()
    parsl.clear()

    assert oneshot_provider.recorded_submits == 1
