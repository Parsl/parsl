from parsl.config import Config
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts


config = Config(
    executors=[
        HighThroughputExecutor(
            label='Comet_HTEX_multinode',
            address=address_by_query(),
            worker_logdir_root=user_opts['comet']['script_dir'],
            max_workers=2,
            provider=SlurmProvider(
                'debug',
                launcher=SrunLauncher(),
                # string to prepend to #SBATCH blocks in the submit
                # script to the scheduler
                scheduler_options=user_opts['comet']['scheduler_options'],

                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['comet']['worker_init'],
                walltime='00:10:00',
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
            ),
        )
    ]
)
