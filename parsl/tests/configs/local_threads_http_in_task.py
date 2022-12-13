from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    return Config(
        executors=[
            ThreadPoolExecutor(
                label='local_threads_http_in_task',
                storage_access=[HTTPInTaskStaging(), NoOpFileStaging()]
            )
        ]
    )
