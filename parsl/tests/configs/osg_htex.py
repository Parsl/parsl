from parsl.config import Config
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

config = Config(
    executors=[
        HighThroughputExecutor(
            label='OSG_HTEX',
            max_workers=1,
            provider=CondorProvider(
                nodes_per_block=1,
                init_blocks=4,
                max_blocks=4,
                # This scheduler option string ensures that the compute nodes provisioned
                # will have modules
                scheduler_options='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init=user_opts['osg']['worker_init'],
                walltime="00:20:00",
            ),
        )
    ]
)
