from parsl.providers import AWSProvider
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
            label='ec2_spot',
            provider=AWSProvider(
                user_opts['ec2']['image_id'],
                region=user_opts['ec2']['region'],
                key_name=user_opts['ec2']['key_name'],
                profile="default",
                state_file='awsproviderstate.json',
                spot_max_bid='1.0',
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                min_blocks=0,
                walltime='00:25:00',
            ),
            controller=Controller(public_ip=user_opts['public_ip']),
        )
    ],
    run_dir=get_rundir(),
)
