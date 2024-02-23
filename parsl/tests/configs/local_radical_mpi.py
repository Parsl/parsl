import os

from parsl.config import Config


def fresh_config():
    from parsl.executors.radical import RadicalPilotExecutor, ResourceConfig

    rpex_cfg = ResourceConfig()
    rpex_cfg.worker_type = "MPI"
    rpex_cfg.worker_cores_per_node = 7

    return Config(
            executors=[
                RadicalPilotExecutor(
                    label='RPEXMPI',
                    rpex_cfg=rpex_cfg,
                    bulk_mode=True,
                    resource='local.localhost',
                    runtime=30, cores=8)])
