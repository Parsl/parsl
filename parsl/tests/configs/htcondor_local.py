from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import CondorProvider
from parsl.launchers import SimpleLauncher


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="docker_htcondor",
                provider=CondorProvider(
                    nodes_per_block=1,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
    )
