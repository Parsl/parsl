"""htex local, but using the proxy store serializer...

DANGER! this will modify the global serializer environment, so any
future parsl stuff done in the same process as this configuration
will not see the default serializer environment...
"""

import os

from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher

# imports for monitoring:
from parsl.monitoring import MonitoringHub
from parsl.providers import LocalProvider

working_dir = os.getcwd() + "/" + "test_htex_alternate"


def fresh_config():
    import parsl.serialize.plugin_proxystore as pspps
    pspps.register_proxystore_serializer()

    return Config(
        executors=[
            HighThroughputExecutor(
                address="127.0.0.1",
                label="htex_Local",
                working_dir=working_dir,
                storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                worker_debug=True,
                cores_per_worker=1,
                heartbeat_period=2,
                heartbeat_threshold=5,
                poll_period=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=5,
                    launcher=SingleNodeLauncher(),
                ),
                block_error_handler=False
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
        ),
        usage_tracking=True
    )


config = fresh_config()
