from parsl.providers import AdHocProvider
from parsl.channels import SSHChannel
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query
from parsl.config import Config

user_opts = {'adhoc':
             {'username': 'YOUR_USERNAME',
              'script_dir': 'YOUR_SCRIPT_DIR',
              'remote_hostnames': ['REMOTE_HOST_URL_1', 'REMOTE_HOST_URL_2']
             }
}

config = Config(
    executors=[
        HighThroughputExecutor(
            label='remote_htex',
            max_workers=2,
            address=address_by_query(),
            worker_logdir_root=user_opts['adhoc']['script_dir'],
            provider=AdHocProvider(
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                channels=[SSHChannel(hostname=m,
                                     username=user_opts['adhoc']['username'],
                                     script_dir=user_opts['adhoc']['script_dir'],
                ) for m in user_opts['adhoc']['remote_hostnames']]
            )
        )
    ],
    #  AdHoc Clusters should not be setup with scaling strategy.
    strategy=None,
)
