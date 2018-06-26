"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| |            | || |            | || |            | || |            | |
| | Task  Task | || | Task  Task | || | Task  Task | || | Task  Task | |
| |            | || |            | || |            | || |            | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================
"""
from libsubmit.providers.slurm.slurm import Slurm
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cori_ipp_multinode',
            provider=Slurm(
                'regular',
                nodes_per_block=4,
                tasks_per_node=2,
                init_blocks=1,
                max_blocks=1,
                overrides=user_opts['cori']['overrides'],
                launcher='srun'
            )
        )

    ],
    run_dir=get_rundir(),
)
