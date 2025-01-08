from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import GridEngineProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label='cc_in2p3_htex',
            max_workers_per_node=2,
            provider=GridEngineProvider(
                nodes_per_block=1,
                init_blocks=2,
                max_blocks=2,
                walltime="00:20:00",
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
