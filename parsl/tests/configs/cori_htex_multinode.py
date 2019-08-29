from parsl.providers import SlurmProvider
from parsl.channels import SSHInteractiveLoginChannel
from parsl.launchers import SrunLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.tests.utils import get_rundir

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from parsl.tests.configs.user_opts import user_opts

config = Config(
    executors=[
        HighThroughputExecutor(
            address=user_opts['public_ip'],
            label='cori_htex_multinode',
            provider=SlurmProvider(
                'debug',
                channel=SSHInteractiveLoginChannel(
                    hostname='cori.nersc.gov',
                    username=user_opts['cori']['username'],
                    script_dir=user_opts['cori']['script_dir']
                ),
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options=user_opts['cori']['scheduler_options'],
                worker_init=user_opts['cori']['worker_init'],
                launcher=SrunLauncher(),
            ),
        )
    ],
    run_dir=get_rundir(),
)
