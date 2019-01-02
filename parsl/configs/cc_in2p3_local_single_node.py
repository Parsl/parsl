"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from parsl.channels import LocalChannel
from parsl.providers import GridEngineProvider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cc_in2p3_local_single_node',
            provider=GridEngineProvider(
                channel=LocalChannel(),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:20:00",
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
            ),
            engine_debug_level='DEBUG',
        )

    ],
)
