from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors import WorkQueueExecutor


def fresh_config():
    return Config(executors=[WorkQueueExecutor(port=9000,
                                               coprocess=True)])
