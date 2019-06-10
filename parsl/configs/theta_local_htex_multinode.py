from parsl.config import Config
from parsl.providers import CobaltProvider
from parsl.launchers import AprunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname


config = Config(
    executors=[
        HighThroughputExecutor(
            label='theta_local_htex_multinode',
            max_workers=4,
            address=address_by_hostname(),
            provider=CobaltProvider(
                queue="debug-flat-quad",
                launcher=AprunLauncher(),
                walltime="00:30:00",
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',           # Specify command to initialize worker environment. For eg:
                # worker_init='source /home/yadunand/setup_theta_env.sh',
                account='ALCF_ALLOCATION',  # Please replace ALCF_ALLOCATION with your ALCF allocation
                cmd_timeout=120,
            ),
        )
    ],

)
