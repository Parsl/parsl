"""
The aim of this configuration is to run a local htex
in a similar manner to htex_local.py, but with lots of
options different and more complicated than in that
configuration, so that more code paths are executed
than when testing only with htex_local.

It does not matter too much *what* is different in this
configuration; what matters is that the differences
cause significantly different pieces of parsl code to be
run - for example, by turning on monitoring, by allowing
blocks to be started by a strategy, by using a different
set of staging providers, by using timing parameters that
will cause substantially different behaviour on whatever
those timing parameters control.
"""

# imports for monitoring:
from parsl.monitoring import MonitoringHub

import os

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor


from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

working_dir = os.getcwd() + "/" + "test_htex_alternate"


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker=1,
                heartbeat_period=2,
                heartbeat_threshold=5,
                poll_period=100,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=5,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        strategy='simple',
        app_cache=True, checkpoint_mode='task_exit',
        retries=2,
        monitoring=MonitoringHub(
                        hub_address="localhost",
                        hub_port=55055,
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
        )
    )


config = fresh_config()
