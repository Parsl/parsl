from parsl.config import Config
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label="midway_htex_multinode",
            worker_debug=False,
            address=address_by_hostname(),
            provider=SlurmProvider(
                'broadwl',
                launcher=SrunLauncher(),
                nodes_per_block=2,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                scheduler_options='',  # string to prepend to #SBATCH blocks in the submit script to the scheduler
                worker_init='',        # command to run before starting a worker, such as 'source activate env'
                walltime='00:30:00'
            ),
        )

    ],
)
