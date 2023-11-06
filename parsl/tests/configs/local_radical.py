import os

from parsl.config import Config
from parsl.executors.radical import RadicalPilotExecutor
from parsl.executors.radical import ResourceConfig

# This is temporary; once everything is merged, we will use Parsl instead of
# this fork.
parsl_src = "pip install git+https://github.com/AymenFJA/parsl.git"
rpex_cfg = ResourceConfig()
rpex_cfg.pilot_env_setup.extend([parsl_src, "pytest", "pandas"])


def fresh_config():

    return Config(
            executors=[
                RadicalPilotExecutor(
                    label='RPEXBulk',
                    rpex_cfg=rpex_cfg,
                    bulk_mode=True,
                    resource='local.localhost',
                    access_schema='local',
                    walltime=30, cores=4)])
