from parsl.config import Config
from parsl.providers import CobaltProvider
from parsl.launchers import AprunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname

from .user_opts import user_opts


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='theta_local_htex_multinode',
                max_workers=1,
                address=address_by_hostname(),
                provider=CobaltProvider(
                    queue=user_opts['theta']['queue'],
                    account=user_opts['theta']['account'],
                    launcher=AprunLauncher(overrides="-d 64"),
                    walltime='00:10:00',
                    nodes_per_block=2,
                    init_blocks=1,
                    max_blocks=1,
                    # string to prepend to #COBALT blocks in the submit
                    # script to the scheduler eg: '#COBALT -t 50'
                    scheduler_options='',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['theta']['worker_init'],
                    cmd_timeout=120,
                ),
            )
        ],
    )


config = fresh_config()
