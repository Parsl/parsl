from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp_multinode',
            provider=SlurmProvider(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/scratch/midway2/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                launcher=SrunLauncher(),
                overrides='OVERRIDES',     # Please replace OVERRIDES with your overrides
                walltime="00:05:00",
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=2,
                tasks_per_node=1,
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )

    ],
)
