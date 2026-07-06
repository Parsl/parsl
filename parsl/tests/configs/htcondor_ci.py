from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import CondorProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="docker_htcondor",
                provider=CondorProvider(
                    worker_init=". ~/venv/bin/activate",
                    nodes_per_block=1,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
    )
