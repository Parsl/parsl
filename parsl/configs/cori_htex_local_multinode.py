""" Cori config running on login node, requesting 2 nodes at a time.
"""

from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
from parsl.config import Config

from parsl.addresses import address_by_interface
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Cori_HTEX_multinode',
            # This is the interface on the login node to which compute
            # nodes can communicate
            address=address_by_interface('bond0.144'),
            max_workers=4,
            cores_per_worker=2,
            provider=SlurmProvider(
                'debug',  # Partition / QOS
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                scheduler_options='''#SBATCH --constraint=knl,quad,cache''',
                worker_init='source ~/setup_parsl_0.8.0_py3.6.sh',
                # We request all hyperthreads on a node.
                launcher=SrunLauncher(overrides='-c 272'),
                walltime="00:20:00",
                # Slurm scheduler on Cori can be slow at times,
                # increase the command timeouts
                cmd_timeout=120,
            ),
        )
    ]
)
