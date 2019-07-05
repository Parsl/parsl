from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp_multicore',
            workers_per_node=4,
            provider=SlurmProvider(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/scratch/midway2/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
                nodes_per_block=1,
                walltime="00:05:00",
                init_blocks=1,
                max_blocks=1,
                launcher=SingleNodeLauncher(),
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )

    ],
)
