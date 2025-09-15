from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Delta_GPU_Executor',
            working_dir='/scratch/path/to/your/directory',
            max_workers_per_node=1,
            provider=SlurmProvider(
                partition='gpuA100x4',
                account='YOUR_ALLOCATION_ON_DELTA',
                constraint="scratch",
                nodes_per_block=1,
                cores_per_node=32,
                mem_per_node=220,
                init_blocks=1,
                max_blocks=1,
                walltime='00:30:00',
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options='#SBATCH --gpus-per-node=1 --gpu-bind=closest',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                exclusive=False,
                launcher=SrunLauncher(),
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
