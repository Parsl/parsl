import os

from parsl.config import Config
from parsl.executors.radical import RadicalPilotExecutor
from parsl.executors.radical import ResourceConfig as rpex_cfg

# This is temporary; once everything is merged, we will use Parsl instead of
# this fork.
parsl_src = "pip install git+https://github.com/AymenFJA/parsl.git"
rpex_cfg.pilot_env_setup.extend([parsl_src, "pytest"])
rpex_cfg.worker_cores_per_node = 7


def fresh_config():
    rpex_cfg.worker_type = "MPI"

    return Config(
            executors=[
                RadicalPilotExecutor(
                    label='RPEXMPI',
                    rpex_cfg=rpex_cfg.get_cfg_file(),
                    bulk_mode=True,
                    resource='local.localhost',
                    access_schema='local',
                    walltime=30, cores=8)])
