from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor

from .user_opts import user_opts


def fresh_config():
    config = Config(
        executors=[
            HighThroughputExecutor(
                label='Midway_HTEX_multinode',
                worker_debug=False,
                address=address_by_hostname(),
                max_workers=1,
                provider=SlurmProvider(
                    'broadwl',  # Partition name, e.g 'broadwl'
                    launcher=SrunLauncher(),
                    nodes_per_block=2,
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler eg: '#SBATCH --constraint=knl,quad,cache'
                    scheduler_options='',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['midway']['worker_init'],
                    walltime='00:30:00',
                    cmd_timeout=120,
                ),
            )
        ],
    )
    return config


config = fresh_config()
