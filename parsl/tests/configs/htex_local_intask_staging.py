from parsl.config import Config
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPInTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_Local",
            worker_debug=True,
            cores_per_worker=1,
            encrypted=True,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
                launcher=SimpleLauncher(),
            ),
            storage_access=[HTTPInTaskStaging(), FTPInTaskStaging(), NoOpFileStaging()]
        )
    ],
    strategy='none',
)
