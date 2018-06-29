"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from libsubmit.providers import SlurmProvider
from libsubmit.channels import SSHChannel

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cori_ipp_single_node',
            provider=SlurmProvider(
                'debug',
                channel=SSHChannel(
                    hostname='cori.nersc.gov',
                    username=user_opts['cori']['username'],
                    script_dir=user_opts['cori']['script_dir']
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                overrides=user_opts['cori']['overrides']
            ),
            controller=Controller(public_ip=user_opts['public_ip']),
        )
    ],
    run_dir=get_rundir(),
)
