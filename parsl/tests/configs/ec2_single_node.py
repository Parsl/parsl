"""Config for EC2.

Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
from libsubmit.providers.aws.aws.EC2Provider import EC2Provider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='ec2_bad_spot',
            provider=EC2Provider(
                user_opts['ec2']['image_id'],
                region=user_opts['ec2']['region'],
                key_name=user_opts['ec2']['key_name'],
                state_file='awsproviderstate.json',
                nodes_per_block=1,
                tasks_per_node=2,
                init_blocks=1,
                max_blocks=1,
                min_blocks=0,
                walltime='01:00:00',
            )
        )
    ],
    run_dir=get_rundir(),
)
