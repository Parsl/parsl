from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider

from .user_opts import user_opts


def fresh_config():
    config = Config(
        executors=[
            HighThroughputExecutor(
                label="bw_htex",
                cores_per_worker=1,
                worker_debug=False,
                max_workers=1,
                address=address_by_hostname(),
                provider=TorqueProvider(
                    queue='normal',
                    launcher=AprunLauncher(),
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler eg: '#SBATCH --constraint=knl,quad,cache'
                    scheduler_options='',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['bluewaters']['worker_init'],
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                    nodes_per_block=2,
                    walltime='00:30:00',
                    cmd_timeout=120,
                ),
            )
        ],
    )
    return config


config = fresh_config()
