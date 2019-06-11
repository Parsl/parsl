"""Config for EC2.

Block {Min:0, init:1, Max:1}
==================
| ++++++++++++++ |
| |    Node    | |
| |            | |
| | Task  Task | |
| |            | |
| ++++++++++++++ |
==================

"""
from parsl.config import Config
from parsl.providers import AWSProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

config = Config(
    executors=[
        HighThroughputExecutor(
            label='ec2_single_node',
            address=address_by_query(),
            provider=AWSProvider(
                image_id='YOUR_AMI_ID',     # Please replace image_id with your image id, e.g., 'ami-82f4dae7'
                region='us-east-1',         # Please replace region with your region
                key_name='YOUR_KEY_NAME',   # Please replace KEY with your key name
                profile="default",          # Please update if not using default profile
                state_file='awsproviderstate.json',
                nodes_per_block=1,
                init_blocks=1,
                walltime='01:00:00',
            ),
        )
    ],
)
