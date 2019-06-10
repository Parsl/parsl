"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

config = Config(
    executors=[
        HighThroughputExecutor(
            label='cc_in2p3_local_single_node',
            address=address_by_query(),
            provider=GridEngineProvider(
                channel=LocalChannel(),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:20:00",
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',           # Input your worker_init if needed
            ),
        )
    ],
)
