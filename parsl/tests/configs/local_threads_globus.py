from parsl.config import Config
from parsl.data_provider.scheme import GlobusScheme
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_globus',
            storage_access=[GlobusScheme(
                endpoint_uuid=user_opts['globus']['endpoint'],
                endpoint_path=user_opts['globus']['path']
            )],
            working_dir=user_opts['globus']['path']
        )
    ],
    run_dir=get_rundir()
)
