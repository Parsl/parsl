from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors.extreme_scale.executor import ExtremeScaleExecutor


def fresh_config():
    return Config(
        executors=[
            ExtremeScaleExecutor(
                label="Extreme_Local",
                worker_debug=True,
                ranks_per_node=4,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                )
            )
        ],
        strategy='none',
    )
