from parsl.config import Config
from parsl.providers import LocalProvider
from parsl.channels import SSHChannel
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor

hostnames = ['host-1', 'host-2']

config = Config(
    executors=[
        HighThroughputExecutor(
            label='htex_{}'.format(h),
            worker_debug=False,
            address=address_by_hostname(),
            provider=LocalProvider(
                # The username on the machines depend on the distribution
                # used, for eg. on Ubuntu, username is 'ubuntu'
                channel=SSHChannel(hostname=h, username='YOUR_USERNAME'),
                move_files=False,  # set to True if there is no shared filesystem
                nodes_per_block=1,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
            ),
        ) for h in hostnames
    ],
    strategy=None
)
