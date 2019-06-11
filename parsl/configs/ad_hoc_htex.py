from parsl.config import Config
from parsl.providers import LocalProvider
from parsl.channels import SSHChannel
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor

username = 'USERNAME'
hostnames = ['host-1', 'host-2']

config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_{}".format(h),
            worker_debug=False,
            address=address_by_hostname(),
            provider=LocalProvider(
                channel=SSHChannel(hostname=h, username=username),
                move_files=False,  # set to True if there is no shared filesystem
                nodes_per_block=1,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                worker_init='',  # command to run before starting a worker, such as 'source activate env'
            ),
        ) for h in hostnames
    ],
    strategy=None
)
