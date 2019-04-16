from parsl.providers import CobaltProvider
from parsl.launchers import AprunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller
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
            label='theta_local_ipp_multinode',
            workers_per_node=1,
            provider=CobaltProvider(
                queue="debug-flat-quad",
                launcher=AprunLauncher(),
                walltime="00:30:00",
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options=user_opts['theta']['scheduler_options'],
                worker_init=user_opts['theta']['worker_init'],
                account=user_opts['theta']['account'],
                cmd_timeout=60
            ),
            controller=Controller(public_ip=user_opts['public_ip'])
        )

    ],
    run_dir=get_rundir(),

)
