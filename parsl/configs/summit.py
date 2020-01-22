from parsl.config import Config
from parsl.executors import HighThroughputExecutor

from parsl.launchers import JsrunLauncher
from parsl.providers import LSFProvider

from parsl.addresses import address_by_interface

config = Config(
    executors=[
        HighThroughputExecutor(
            label='Summit_HTEX',
            # On Summit ensure that the working dir is writeable from the compute nodes,
            # for eg. paths below /gpfs/alpine/world-shared/
            working_dir='YOUR_WORKING_DIR_ON_SHARED_FS',
            address=address_by_interface('ib0'),  # This assumes Parsl is running on login node
            worker_port_range=(50000, 55000),
            provider=LSFProvider(
                launcher=JsrunLauncher(),
                walltime="00:10:00",
                nodes_per_block=2,
                init_blocks=1,
                max_blocks=1,
                worker_init='',  # Input your worker environment initialization commands
                project='YOUR_PROJECT_ALLOCATION',
                cmd_timeout=60
            ),
        )

    ],
)
