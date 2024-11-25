from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                loopback_address="::ffff:127.0.0.1",
                worker_debug=True,
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy='none',
    )
