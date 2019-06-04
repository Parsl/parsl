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
from parsl.tests.utils import get_rundir

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

config = Config(
    executors=[
        IPyParallelExecutor(
            label='beagle_multinode_mpi',
            workers_per_node=1,
            provider=TorqueProvider(
                queue='debug',
                channel=SSHChannel(
                    hostname='login4.beagle.ci.uchicago.edu',
                    username=user_opts['beagle']['username'],
                    script_dir="/lustre/beagle2/{}/parsl_scripts".format(user_opts['beagle']['username'])
                ),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher(),
                scheduler_options=user_opts['beagle']['scheduler_options'],
                worker_init=user_opts['beagle']['worker_init'],
            )
        )

    ],
    run_dir=get_rundir()
)
