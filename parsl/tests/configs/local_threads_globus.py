from parsl.config import Config
from parsl.data_provider.scheme import GlobusScheme
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.user_opts import user_opts  # must be configured specifically for each user
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_globus',
            storage_access=GlobusScheme(
                endpoint_uuid=user_opts['globus']['endpoint'],
                endpoint_path=user_opts['globus']['path']
            ),
            working_dir=user_opts['globus']['path']
        )
    ],
    run_dir=get_rundir()
)
