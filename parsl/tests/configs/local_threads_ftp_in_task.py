from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_http_in_task',
            storage_access=[FTPInTaskStaging(), NoOpFileStaging()]
        )
    ]
)
