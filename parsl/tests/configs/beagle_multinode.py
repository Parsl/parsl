"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| |            | || |            | || |            | || |            | |
| | Task  Task | || | Task  Task | || | Task  Task | || | Task  Task | |
| |            | || |            | || |            | || |            | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
from libsubmit.channels.ssh.ssh import SSHChannel
from libsubmit.providers.slurm.slurm import Slurm
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts  # must be configured specifically for each user


config = Config(
    executors=[
        IPyParallelExecutor(
            provider=Slurm(
                'debug',
                channel=SSHChannel(
                    hostname='beagle.nersc.gov',
                    username=user_opts['beagle']['username'],
                    script_dir=user_opts['beagle']['script_dir']
                ),
                launcher='srun',
                nodes_per_block=4,
                tasks_per_node=2,
                overrides=user_opts['beagle']['overrides']
            ),
            label='beagle_multinode'
        )
    ]
)
