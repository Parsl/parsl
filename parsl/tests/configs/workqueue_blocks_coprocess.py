from parsl.config import Config
from parsl.executors import WorkQueueExecutor

from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

from parsl.providers import LocalProvider
from parsl import MonitoringHub

config = Config(
                monitoring=MonitoringHub(
                        hub_address="localhost",
                        hub_port=55055,
                        monitoring_debug=False,
                        resource_monitoring_interval=1,
                ),
                executors=[WorkQueueExecutor(port=0, coprocess=True,
                                             storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()],
                                             provider=LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1))])
