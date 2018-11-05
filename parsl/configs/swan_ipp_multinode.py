"""
    Block
====================================
| ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | |
| |            | || |            | |
| | Task  Task | || | Task  Task | |
| |            | || |            | |
| ++++++++++++++ || ++++++++++++++ |
====================================
"""
from parsl.channels import SSHChannel
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller


# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='swan_ipp',
            provider=TorqueProvider(
                channel=SSHChannel(
                    hostname='swan.cray.com',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/home/users/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                nodes_per_block=2,
                tasks_per_node=2,
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher(),
                overrides='OVERRIDES',     # Please replace OVERRIDES with your overrides
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )

    ],
)
