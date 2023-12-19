import os

from parsl.config import Config
from parsl.executors.radical import RadicalPilotExecutor
from parsl.executors.radical import ResourceConfig


rpex_cfg = ResourceConfig()


def fresh_config():

    return Config(
            executors=[
                RadicalPilotExecutor(
                    label='RPEXBulk',
                    rpex_cfg=rpex_cfg,
                    bulk_mode=True,
                    resource='local.localhost',
                    runtime=30, cores=4)])
