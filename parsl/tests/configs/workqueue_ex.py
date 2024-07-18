from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors import WorkQueueExecutor
from parsl.monitoring import MonitoringHub
from parsl.providers import LocalProvider


def fresh_config():
    return Config(executors=[WorkQueueExecutor(port=9000,
                                               coprocess=True,
                                               provider=LocalProvider(init_blocks=0, min_blocks=0, max_blocks=1)
                                               )],
                  monitoring=MonitoringHub(hub_address="localhost",
                                           hub_port=55055,
                                           monitoring_debug=True,
                                           resource_monitoring_interval=1,
                                           ))
