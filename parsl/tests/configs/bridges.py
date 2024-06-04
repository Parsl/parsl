from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider

from .user_opts import user_opts


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='Bridges_HTEX_multinode',
                # This is the network interface on the login node to
                # which compute nodes can communicate
                # address=address_by_interface('bond0.144'),
                max_workers_per_node=1,
                encrypted=True,
                provider=SlurmProvider(
                    user_opts['bridges']['partition'],  # Partition / QOS
                    nodes_per_block=2,
                    init_blocks=1,
                    # string to prepend to #SBATCH blocks in the submit
                    # script to the scheduler eg: '#SBATCH --constraint=knl,quad,cache'
                    scheduler_options=user_opts['bridges']['scheduler_options'],

                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init=user_opts['bridges']['worker_init'],

                    # We request all hyperthreads on a node.
                    launcher=SrunLauncher(),
                    walltime='00:10:00',
                    # Slurm scheduler on Cori can be slow at times,
                    # increase the command timeouts
                    cmd_timeout=120,
                ),
            )
        ]
    )
