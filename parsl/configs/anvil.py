from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Anvil_GPU_Executor',
            working_dir='/anvil/scratch/path/to/your/directory',
            max_workers_per_node=1,
            provider=SlurmProvider(
                partition='gpu',
                account='YOUR_ALLOCATION_ON_ANVIL',
                nodes_per_block=1,
                cores_per_node=4,
                mem_per_node=50,
                init_blocks=1,
                max_blocks=1,
                walltime='00:30:00',
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options='#SBATCH --gres gpu:1 --gpus-per-node=1',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='module load modtree/gpu',
                exclusive=False,
                launcher=SrunLauncher(),
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
