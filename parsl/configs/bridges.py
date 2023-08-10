from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface

""" This config assumes that it is used to launch parsl tasks from the login nodes
of Bridges at PSC. Each job submitted to the scheduler will request 2 nodes for 10 minutes.
"""

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Bridges_HTEX_multinode',
            address=address_by_interface('ens3f0'),
            max_workers=1,
            provider=SlurmProvider(
                'YOUR_PARTITION_NAME',  # Specify Partition / QOS, for eg. RM-small
                nodes_per_block=2,
                init_blocks=1,
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#SBATCH --gres=gpu:type:n'
                scheduler_options='',

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',

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
