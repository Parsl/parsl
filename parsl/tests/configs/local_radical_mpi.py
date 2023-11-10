import os

from parsl.config import Config
from parsl.executors.radical import RadicalPilotExecutor
from parsl.executors.radical import ResourceConfig


rpex_cfg = ResourceConfig()
rpex_cfg.worker_type = "MPI"
rpex_cfg.worker_cores_per_node = 7


def fresh_config():

    return Config(
            executors=[
                RadicalPilotExecutor(
                    label='RPEXMPI',
                    rpex_cfg=rpex_cfg,
                    bulk_mode=True,
                    resource='local.localhost',
                    walltime=30, cores=8)])
