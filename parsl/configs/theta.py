from parsl.config import Config
from parsl.providers import CobaltProvider
from parsl.launchers import AprunLauncher
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_interface

config = Config(
    executors=[
        HighThroughputExecutor(
            label='theta_local_htex_multinode',
            address=address_by_interface('vlan2360'),
            max_workers=4,
            cpu_affinity='block',  # Ensures that workers use cores on the same tile
            provider=CobaltProvider(
                queue='YOUR_QUEUE',
                account='YOUR_ACCOUNT',
                launcher=AprunLauncher(overrides="-d 64 --cc depth"),
                walltime='00:30:00',
                nodes_per_block=2,
                init_blocks=1,
                min_blocks=1,
                max_blocks=1,
                # string to prepend to #COBALT blocks in the submit
                # script to the scheduler eg: '#COBALT -t 50'
                scheduler_options='',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                cmd_timeout=120,
            ),
        )
    ],
)
