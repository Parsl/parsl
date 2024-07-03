from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig


def fresh_config():
    return Config(executors=[TaskVineExecutor(manager_config=TaskVineManagerConfig(port=9000),
                                              worker_launch_method='factory')])
