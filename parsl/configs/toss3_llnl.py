from parsl.config import Config
from parsl.executors import FluxExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        FluxExecutor(
            provider=SlurmProvider(
                partition="YOUR_PARTITION",  # e.g. "pbatch", "pdebug"
                account="YOUR_ACCOUNT",
                launcher=SrunLauncher(overrides="--mpibind=off"),
                nodes_per_block=1,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                walltime="00:30:00",
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler, e.g.: '#SBATCH -t 50'
                scheduler_options='',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                cmd_timeout=120,
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
