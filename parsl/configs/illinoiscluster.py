from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.usage_tracking.levels import LEVEL_1

""" This config assumes that it is used to launch parsl tasks from the login nodes
of the Campus Cluster at UIUC. Each job submitted to the scheduler will request 2 nodes for 10 minutes.
"""
config = Config(
     executors=[
          HighThroughputExecutor(
               label="CC_htex",
               worker_debug=False,
               cores_per_worker=16.0,  # each worker uses a full node
               provider=SlurmProvider(
                    partition='secondary-fdr',  # partition
                    nodes_per_block=2,  # number of nodes
                    init_blocks=1,
                    max_blocks=1,
                    scheduler_options='',
                    cmd_timeout=60,
                    walltime='00:10:00',
                    launcher=SrunLauncher(),
                    worker_init='conda activate envParsl',  # requires conda environment with parsl
               ),
          )
     ],
     usage_tracking=LEVEL_1,
)
