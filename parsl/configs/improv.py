from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiRunLauncher
from parsl.providers import PBSProProvider

config = Config(
        executors=[
            HighThroughputExecutor(
                label="Improv_multinode",
                max_workers_per_node=32,
                provider=PBSProProvider(
                    account="YOUR_ALLOCATION_ON_IMPROV",
                    # PBS directives (header lines), for example:
                    # scheduler_options='#PBS -l mem=4gb',
                    scheduler_options='',

                    queue="compute",

                    # Command to be run before starting a worker:
                    # **WARNING** Improv requires an openmpi module to be
                    # loaded for the MpiRunLauncher. Add additional env
                    # load commands to this multiline string.
                    worker_init='''
module load gcc/13.2.0;
module load openmpi/5.0.3-gcc-13.2.0; ''',
                    launcher=MpiRunLauncher(),

                    # number of compute nodes allocated for each block
                    nodes_per_block=2,
                    walltime='00:10:00'
                ),
            ),
        ],
)
