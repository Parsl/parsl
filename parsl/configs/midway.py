from parsl.addresses import address_by_interface
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Midway_HTEX_multinode',
            address=address_by_interface('bond0'),
            worker_debug=False,
            max_workers_per_node=2,
            provider=SlurmProvider(
                'YOUR_PARTITION',  # Partition name, e.g 'broadwl'
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
                worker_init='',
                walltime='00:30:00'
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
