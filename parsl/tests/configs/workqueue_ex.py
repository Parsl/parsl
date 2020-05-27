from parsl.config import Config
from parsl.executors import WorkQueueExecutor

from parsl.data_provider.http import HTTPInTaskStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.file_noop import NoOpFileStaging

config = Config(executors=[WorkQueueExecutor(port=9000,
                                             storage_access=[FTPInTaskStaging(), HTTPInTaskStaging(), NoOpFileStaging()])])
