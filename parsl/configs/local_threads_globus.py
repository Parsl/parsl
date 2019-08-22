from parsl.config import Config
from parsl.data_provider.globus import GlobusStaging
from parsl.executors.threads import ThreadPoolExecutor

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_globus',
            storage_access=[GlobusStaging(
                endpoint_uuid='UUID',    # Please replace UUID with your uuid
                endpoint_path='PATH'    # Please replace PATH with your path
            )],
            working_dir='PATH'    # Please replace PATH with your path
        )
    ],
)
