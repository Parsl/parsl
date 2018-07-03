from libsubmit.channels import SSHChannel
from libsubmit.providers import SlurmProvider
from libsubmit.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp_multicore',
            provider=SlurmProvider(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username=user_opts['midway']['username'],
                    script_dir=user_opts['midway']['script_dir']
                ),
                overrides=user_opts['midway']['overrides'],
                nodes_per_block=1,
                tasks_per_node=4,
                walltime="00:05:00",
                init_blocks=1,
                max_blocks=1,
                launcher=SingleNodeLauncher(),
            ),
            controller=Controller(public_ip=user_opts['public_ip']),
        )

    ],
    run_dir=get_rundir()
)
