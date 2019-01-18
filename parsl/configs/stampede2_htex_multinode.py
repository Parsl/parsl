from parsl.config import Config

from parsl.channels import LocalChannel
from parsl.providers import SlurmProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname

from parsl.data_provider.scheme import GlobusScheme

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')


config = Config(
    executors=[
        HighThroughputExecutor(
            label="stampede2_htex",
            worker_debug=False,
            address=address_by_hostname(),
            provider=SlurmProvider(
                channel=LocalChannel(),
                nodes_per_block=2,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                partition='PARTITION',  # Replace with partition name
                scheduler_options='',   # Enter scheduler_options if needed
                worker_init='',         # Enter worker_init if needed
                walltime='00:30:00'
            ),
            storage_access=[GlobusScheme(
                endpoint_uuid="ceea5ca0-89a9-11e7-a97f-22000a92523b",
                endpoint_path="/",
                local_path="/"
            )]
        )

    ],
)
