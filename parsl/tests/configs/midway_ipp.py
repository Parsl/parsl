from parsl.tests.user_opts import user_opts

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.channels.ssh.ssh import SSHChannel
from libsubmit.providers.slurm.slurm import Slurm

config = Config(
    executors=[
        IPyParallelExecutor(
            provider=Slurm(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username=user_opts['midway']['username'],
                    script_dir=user_opts['midway']['script_dir']
                ),
                init_blocks=1,
                min_blocks=1,
                max_blocks=2,
                nodes_per_block=1,
                tasks_per_node=4,
                parallelism=0.5,
                overrides=user_opts['midway']['overrides']
            ),
            label='midway_ipp'
        )
    ]
)
