import threading

import pytest

import parsl
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


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
                address="127.0.0.1",
                worker_debug=True,
                cores_per_worker=1,
                provider=oneshot_provider,
                worker_logdir_root=str(tmpd_cwd)
            )
        ],
        strategy='simple',
    )

    parsl.load(config)
    dfk = parsl.dfk()

    def poller():
        import time
        while True:
            dfk.job_status_poller.poll()
            time.sleep(0.1)

    threading.Thread(target=poller, daemon=True).start()
    app().result()
    parsl.dfk().cleanup()
    parsl.clear()

    assert oneshot_provider.recorded_submits == 1
