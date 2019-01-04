"""
================== Block
| ++++++++++++++ | Node
| |            | |
| |    Task    | |             . . .
| |            | |
| ++++++++++++++ |
==================
"""
from parsl.channels import SSHChannel
from parsl.providers import TorqueProvider
from parsl.launchers import AprunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='beagle_multinode_mpi',
            provider=TorqueProvider(
                'debug',
                channel=SSHChannel(
                    hostname='login4.beagle.ci.uchicago.edu',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/lustre/beagle2/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher(),
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
            )
        )

    ],
)
