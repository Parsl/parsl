from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

# Ensure that the following variable is set to your username on Comet
USERNAME = ''
# Ensure that parsl is installed in an environment on Comet and
# provider a command on script to WORKER_INIT to activate that env.
WORKER_INIT = 'source activate parsl_0.8.0a0_py3.6',

config = Config(
    executors=[
        HighThroughputExecutor(
            label='comet_ipp_multinode',
            address=address_by_query(),
            max_workers=4,
            worker_logdir_root="parsl_logdir",
            provider=SlurmProvider(
                'debug',
                channel=SSHChannel(
                    hostname='comet.sdsc.xsede.org',
                    username=USERNAME,
                    script_dir='/home/{}/parsl_scripts'.format(USERNAME),
                ),
                launcher=SrunLauncher(),
                worker_init=WORKER_INIT,
                walltime="00:10:00",
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
            ),
        )
    ]
)
