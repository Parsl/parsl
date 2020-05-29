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
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.tests.utils import get_rundir

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

config = Config(
    executors=[
        HighThroughputExecutor(
            label='swan_htex',
            provider=TorqueProvider(
                channel=SSHChannel(
                    hostname='swan.cray.com',
                    username=user_opts['swan']['username'],
                    script_dir=user_opts['swan']['script_dir'],
                ),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                launcher=AprunLauncher(),
                scheduler_options=user_opts['swan']['scheduler_options'],
                worker_init=user_opts['swan']['worker_init'],
            ),
        )
    ],
    run_dir=get_rundir()
)
