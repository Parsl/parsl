from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="docker_slurm",
                encrypted=True,
                provider=SlurmProvider(
                    cmd_timeout=60,     # Add extra time for slow scheduler responses
                    nodes_per_block=1,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    walltime='00:10:00',
                    launcher=SrunLauncher(),
                ),
            )
        ],
    )
