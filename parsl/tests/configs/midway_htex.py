from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
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
            label='midway_htex_multinode',
            provider=SlurmProvider(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username=user_opts['midway']['username'],
                    script_dir=user_opts['midway']['script_dir']
                ),
                launcher=SrunLauncher(),
                scheduler_options=user_opts['midway']['scheduler_options'],
                worker_init=user_opts['midway']['worker_init'],
                walltime="00:05:00",
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
            ),
        )

    ],
    run_dir=get_rundir()
)
