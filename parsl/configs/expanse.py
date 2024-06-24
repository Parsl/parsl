from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Expanse_CPU_Multinode',
            max_workers_per_node=32,
            provider=SlurmProvider(
                'compute',
                account='YOUR_ALLOCATION_ON_EXPANSE',
                launcher=SrunLauncher(),
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options='',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                walltime='01:00:00',
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
