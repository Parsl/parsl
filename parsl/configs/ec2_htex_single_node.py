from parsl.config import Config
from parsl.providers import AWSProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

config = Config(
    executors=[
        HighThroughputExecutor(
            label='ec2_htex_single_node',
            address=address_by_query(),
            provider=AWSProvider(
                image_id='YOUR_AMI_ID',
                region='us-east-1',
                key_name='YOUR_KEY_NAME',
                profile='default',
                state_file='awsproviderstate.json',
                nodes_per_block=1,
                init_blocks=1,
                walltime='01:00:00',
            ),
        )
    ],
)
