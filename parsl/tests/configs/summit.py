from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import JsrunLauncher
from parsl.providers import LSFProvider

from .user_opts import user_opts
""" This config assumes that it is used to launch parsl tasks from the login nodes
of Frontera at TACC. Each job submitted to the scheduler will request 2 nodes for 10 minutes.
"""


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='Summit_HTEX',
                # On Summit ensure that the working dir is writeable from the compute nodes,
                # for eg. paths below /gpfs/alpine/world-shared/
                # working_dir='',

                # address=address_by_interface('ib0'),  # This assumes Parsl is running on login node
                worker_port_range=(50000, 55000),
                max_workers=1,
                provider=LSFProvider(
                    launcher=JsrunLauncher(),
                    walltime="00:10:00",
                    nodes_per_block=2,
                    init_blocks=1,
                    max_blocks=1,
                    worker_init=user_opts['summit']['worker_init'],
                    project=user_opts['summit']['project'],
                    cmd_timeout=60
                ),
            )
        ],
    )
