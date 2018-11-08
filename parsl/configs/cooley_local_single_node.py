# Untested

from parsl.providers import CobaltProvider
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
            label='cooley_local_single_node',
            provider=CobaltProvider(
                launcher=SingleNodeLauncher(),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:05:00",
                scheduler_options='SCHEDULER_OPTIONS',     # Please replace SCHEDULER_OPTIONS with your scheduler_options
                worker_init='WORKER_INIT',     # Please replace WORKER_INIT with your worker_init
                queue='debug',
                account='ALCF_ALLOCATION',    # Please replace ALCF_ALLOCATION with your ALCF allocation
            ),
            controller=Controller(public_ip="10.230.100.210")
        )

    ],
)
