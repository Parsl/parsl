from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                worker_debug=True,
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy=None,
    )


config = fresh_config()
