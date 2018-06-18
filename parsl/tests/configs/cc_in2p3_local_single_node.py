"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from libsubmit.channels.local.local import LocalChannel
from libsubmit.providers.grid_engine.grid_engine import GridEngine
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cc_in2p3_local_single_node',
            provider=GridEngine(
                channel=LocalChannel(
                    script_dir=user_opts['cc_in2p3']['script_dir']
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                overrides=user_opts['cc_in2p3']['overrides'],
            )
        )

    ],
    run_dir=get_rundir()
)
