from libsubmit.channels.ssh.ssh import SSHChannel
from libsubmit.providers.slurm.slurm import Slurm
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp_multicore',
            provider=Slurm(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username=user_opts['midway']['username'],
                    script_dir="/scratch/midway2/{0}/parsl_scripts".format(user_opts['midway']['username'])
                ),
                overrides=user_opts['midway']['overrides'],
                nodes_per_block=1,
                tasks_per_node='$CORES',
                walltime="00:05:00",
                init_blocks=1,
                max_blocks=1
            )
        )

    ],
    run_dir=get_rundir()
)
