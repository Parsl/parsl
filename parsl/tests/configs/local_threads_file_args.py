import os

from parsl.config import Config
from parsl.data_provider.file_args import FileArgsStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_file_args',
            storage_access=[FileArgsStaging(), HTTPInTaskStaging()],
            working_dir=os.getcwd() + "/test_file_args_workdir"
        )
    ],
    run_dir=get_rundir()
)
