# UNTESTED

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.launchers import MpiRunLauncher
from parsl.providers import CobaltProvider
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
            label="cooley_htex",
            worker_debug=False,
            cores_per_worker=1,
            address=address_by_hostname(),
            provider=CobaltProvider(
                queue='debug',
                account=user_opts['cooley']['account'],
                launcher=MpiRunLauncher(),  # UNTESTED COMPONENT
                scheduler_options=user_opts['cooley']['scheduler_options'],
                worker_init=user_opts['cooley']['worker_init'],
                init_blocks=1,
                max_blocks=1,
                min_blocks=1,
                nodes_per_block=4,
                cmd_timeout=60,
                walltime='00:10:00',
            ),
        )
    ],
    run_dir=get_rundir(),
)
