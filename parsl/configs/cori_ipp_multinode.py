"""
    Block {Min:0, init:1, Max:1}
====================================
| ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | |
| |            | || |            | |
| | Task  Task | || | Task  Task | |
| |            | || |            | |
| ++++++++++++++ || ++++++++++++++ |
====================================
"""
from parsl.providers import SlurmProvider
from parsl.channels import SSHChannel
from parsl.launchers import SrunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cori_ipp_multinode',
            provider=SlurmProvider(
                'debug',
                channel=SSHChannel(
                    hostname='cori.nersc.gov',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/global/homes/y/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
                launcher=SrunLauncher(),
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )
    ],
)
