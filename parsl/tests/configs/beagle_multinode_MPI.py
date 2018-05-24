"""
                      Block {Min:0, init:1, Max:1}
========================================================================
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
| |    Node    | || |    Node    | || |    Node    | || |    Node    | |
| | +--------------------------+ | || | +--------------------------+ | |
| | |         MPI Task         | | || | |         MPI Task         | | |
| | +--------------------------+ | || | +--------------------------+ | |
| ++++++++++++++ || ++++++++++++++ || ++++++++++++++ || ++++++++++++++ |
========================================================================

"""
from libsubmit.channels.ssh.ssh import SSHChannel
from libsubmit.providers.slurm.slurm import Slurm
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='beagle_multinode_mpi',
            provider=Slurm(
                'debug',
                channel=SSHChannel(
                    hostname='beagle.nersc.gov',
                    username=user_opts['beagle']['username'],
                    script_dir=user_opts['beagle']['script_dir']
                ),
                walltime="00:10:00",
                nodes_per_block=4,
                tasks_per_node=1 / 2.,
                init_blocks=1,
                max_blocks=1,
                launcher='srun',
                overrides=user_opts['beagle']['overrides'],
            )
        )

    ],
    run_dir=get_rundir()
)
