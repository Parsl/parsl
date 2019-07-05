from parsl.providers import AWSProvider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='ec2_spot',
            provider=AWSProvider(
                'image_id',    # Please replace image_id with your image id, e.g., 'ami-82f4dae7'
                region='us-east-1',    # Please replace region with your region
                key_name='KEY',    # Please replace KEY with your key name
                profile="default",
                state_file='awsproviderstate.json',
                spot_max_bid='1.0',
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                min_blocks=0,
                walltime='00:25:00',
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )
    ],
)
