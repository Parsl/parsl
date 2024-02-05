from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher

from .user_opts import user_opts
""" This config assumes that it is used to launch parsl tasks from the login nodes
of Frontera at TACC. Each job submitted to the scheduler will request 2 nodes for 10 minutes.
"""


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="frontera_htex",
                max_workers=1,
                encrypted=True,
                provider=SlurmProvider(
                    cmd_timeout=60,     # Add extra time for slow scheduler responses
                    channel=LocalChannel(),
                    nodes_per_block=2,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    partition='development',  # Replace with partition name
                    scheduler_options=user_opts['frontera']['scheduler_options'],

                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['frontera']['worker_init'],

                    # Ideally we set the walltime to the longest supported walltime.
                    walltime='00:10:00',
                    launcher=SrunLauncher(),
                ),
            )
        ],
    )


config = fresh_config()
