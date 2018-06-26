"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from libsubmit.channels.ssh.ssh import SSHChannel
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers.torque import Torque
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='swan_ipp',
            provider=Torque(
                channel=SSHChannel(
                    hostname='swan.cray.com',
                    username=user_opts['swan']['username'],
                    script_dir="/home/users/{}/parsl_scripts".format(user_opts['swan']['username'])
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                launcher='aprun',
                overrides=user_opts['swan']['overrides']
            )
        )

    ],
    run_dir=get_rundir()
)
