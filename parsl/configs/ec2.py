from parsl.addresses import address_by_route
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import AWSProvider
from parsl.usage_tracking.levels import LEVEL_1

# This configuration assumes that the Parsl script runs on an EC2 instance
# inside the VPC created by AWSProvider, so that the workers can reach it at
# its private address. Credentials come from boto3's default credential chain
# (for example AWS_PROFILE or an instance role) unless profile or key_file is set.
config = Config(
    executors=[
        HighThroughputExecutor(
            label='ec2_single_node',
            # An address of this host which the EC2 instances can reach
            address=address_by_route(),
            worker_port=54000,
            # The CurveZMQ certificates are not available on the EC2 instances
            encrypted=False,
            provider=AWSProvider(
                # Specify your EC2 AMI id. It must provide the same Python
                # major.minor version as this host, e.g. Python 3.12 on Ubuntu 24.04
                'YOUR_AMI_ID',
                # Specify the AWS region to provision from
                # eg. us-east-1
                region='YOUR_AWS_REGION',

                # Specify the name of the key to allow ssh access to nodes,
                # or None to launch instances without a key pair
                key_name='YOUR_KEY_NAME',
                instance_type='t3.small',
                state_file='awsproviderstate.json',
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                min_blocks=0,
                # Instances shut down after this time
                walltime='01:00:00',
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
