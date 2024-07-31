from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.zip import ZipFileStaging
from parsl.executors.taskvine import TaskVineExecutor, TaskVineManagerConfig
from parsl.executors.taskvine.stub_staging_provider import StubStaging


def fresh_config():
    return Config(executors=[TaskVineExecutor(manager_config=TaskVineManagerConfig(port=9000),
                                              worker_launch_method='factory',
                                              storage_access=[FTPInTaskStaging(), StubStaging(), NoOpFileStaging(), ZipFileStaging()])])
