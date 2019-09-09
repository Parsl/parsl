import logging

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SimpleLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor

from parsl.monitoring import MonitoringHub

from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_Local",
            storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
            worker_debug=True,
            cores_per_worker=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=5,
                launcher=SimpleLauncher(),
            ),
        )
    ],
    strategy='simple',
    app_cache=True, checkpoint_mode='task_exit',
    monitoring=MonitoringHub(
                    hub_address="localhost",
                    hub_port=55055,
                    logging_level=logging.INFO,
                    resource_monitoring_interval=10,
                ),
)
