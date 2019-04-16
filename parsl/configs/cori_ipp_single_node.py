"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from parsl.providers import SlurmProvider
from parsl.channels import SSHChannel

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cori_ipp_single_node',
            provider=SlurmProvider(
                'debug',
                channel=SSHChannel(
                    hostname='cori.nersc.gov',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/global/homes/y/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )
    ],
)
