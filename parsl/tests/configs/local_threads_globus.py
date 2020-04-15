from parsl.config import Config
from parsl.data_provider.data_manager import default_staging
from parsl.data_provider.globus import GlobusStaging
from parsl.executors.threads import ThreadPoolExecutor

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

storage_access = default_staging + [GlobusStaging(
                endpoint_uuid=user_opts['globus']['endpoint'],
                endpoint_path=user_opts['globus']['path']
            )]

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_globus',
            working_dir=user_opts['globus']['path'],
            storage_access=storage_access
        )
    ]
)

remote_writeable = user_opts['globus']['remote_writeable']
