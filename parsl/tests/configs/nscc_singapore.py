from parsl.providers import PBSProProvider
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiRunLauncher
from parsl.addresses import address_by_interface
from parsl.config import Config

from .user_opts import user_opts


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex",
                heartbeat_period=15,
                heartbeat_threshold=120,
                worker_debug=False,
                max_workers=1,
                address=address_by_interface('ib0'),
                provider=PBSProProvider(
                    launcher=MpiRunLauncher(),
                    # string to prepend to #PBS blocks in the submit
                    # script to the scheduler
                    # E.g., project name
                    scheduler_options=user_opts['nscc']['scheduler_options'],
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['nscc']['worker_init'],
                    nodes_per_block=2,
                    min_blocks=1,
                    max_blocks=1,
                    cpus_per_node=24,
                    walltime="00:20:00",
                    cmd_timeout=300,
                ),
            ),
        ],
        strategy='simple',
    )


config = fresh_config()
