from parsl.config import Config
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query


config = Config(
    executors=[
        HighThroughputExecutor(
            label='CC.IN2P3_HTEX',
            address=address_by_query(),
            provider=GridEngineProvider(
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler eg: '#$ -M YOUR_EMAIL@gmail.com
                scheduler_options='',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                walltime='00:20:00',
            ),
        )
    ],
)
