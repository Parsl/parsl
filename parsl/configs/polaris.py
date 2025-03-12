from parsl.addresses import address_by_interface
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiExecLauncher
from parsl.providers import PBSProProvider
from parsl.usage_tracking.levels import LEVEL_1

# There are three user parameters to change for the PBSProProvider:
#  YOUR_ACCOUNT: Account to charge usage
#  YOUR_ENV: Name or path of your Parsl environment
#  YOUR_WALLTIME: How long each job should run for

#  You may also need to change the filesystem if your data is not on Eagle

config = Config(
    executors=[
        HighThroughputExecutor(
            available_accelerators=4,  # Ensures one worker per accelerator
            address=address_by_interface('bond0'),
            cpu_affinity="alternating",  # Prevents thread contention
            prefetch_capacity=0,  # Increase if you have many more tasks than workers
            provider=PBSProProvider(
                account="YOUR_ACCOUNT",
                worker_init="module load conda; conda activate YOUR_ENV",
                walltime="YOUR_WALLTIME",
                queue="prod",  # Small runs should use "debug" or "debug-scaling"
                scheduler_options="#PBS -l filesystems=home:eagle",  # Change if data on other filesystem
                launcher=MpiExecLauncher(
                    bind_cmd="--cpu-bind", overrides="--depth=64 --ppn 1"
                ),  # Ensures 1 manger per node and allows it to divide work to all 64 cores
                select_options="ngpus=4",
                nodes_per_block=128,
                min_blocks=0,
                max_blocks=1,
                cpus_per_node=64,
            ),
        ),
    ],
    usage_tracking=LEVEL_1,
)
