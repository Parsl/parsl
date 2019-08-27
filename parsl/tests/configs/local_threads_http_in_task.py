from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_http_in_task',
            storage_access=[HTTPInTaskStaging(), NoOpFileStaging()]
        )
    ],
    run_dir=get_rundir()
)
