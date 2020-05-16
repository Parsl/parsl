from parsl.config import Config
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query
from .user_opts import user_opts


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='Comet_HTEX_multinode',
                address=address_by_query(),
                max_workers=1,
                provider=SlurmProvider(
                    'debug',
                    launcher=SrunLauncher(),
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler
                    scheduler_options=user_opts['comet']['scheduler_options'],

                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['comet']['worker_init'],
                    walltime='00:10:00',
                    init_blocks=1,
                    max_blocks=1,
                    nodes_per_block=2,
                ),
            )
        ]
    )


config = fresh_config()
