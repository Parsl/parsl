from parsl.providers import CobaltProvider
from parsl.launchers import AprunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='theta_local_ipp_multinode',
            workers_per_node=1,
            provider=CobaltProvider(
                queue="debug-flat-quad",
                launcher=AprunLauncher(),
                walltime="00:30:00",
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
                account='ALCF_ALLOCATION',    # Please replace ALCF_ALLOCATION with your ALCF allocation
                cmd_timeout=60
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )

    ],

)
