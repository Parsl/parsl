from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import LowLatencyExecutor


def llex_config(workers_per_node, num_blocks):
    config = Config(
        executors=[
            LowLatencyExecutor(
                label="llex_local",
                # worker_debug=True,
                workers_per_node=workers_per_node,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=num_blocks,
                    max_blocks=num_blocks,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        strategy=None,
    )
    return config


config = llex_config(1, 1)
