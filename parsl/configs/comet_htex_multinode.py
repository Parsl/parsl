from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query


config = Config(
    executors=[
        HighThroughputExecutor(
            label='comet_htex_multinode',
            address=address_by_query(),
            max_workers=4,
            worker_logdir_root="parsl_logdir",
            provider=SlurmProvider(
                'debug',
                channel=SSHChannel(
                    hostname='comet.sdsc.xsede.org',
                    username='YOUR_USERNAME',
                    script_dir='YOUR_SCRIPTDIR',
                ),
                launcher=SrunLauncher(),
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options='',
                # Command to be run before starting a worker, such as :
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                walltime="00:10:00",
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
            ),
        )
    ]
)
