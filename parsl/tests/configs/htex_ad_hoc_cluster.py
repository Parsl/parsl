from parsl.providers import AdHocProvider
from parsl.channels import SSHChannel
from parsl.executors import HighThroughputExecutor

from parsl.config import Config

from parsl.tests.configs.user_opts import user_opts

config = Config(
    executors=[
        HighThroughputExecutor(
            label='remote_htex',
            cores_per_worker=1,
            worker_debug=False,
            address=user_opts['public_ip'],
            provider=AdHocProvider(
                move_files=False,
                parallelism=1,
                worker_init=user_opts['adhoc']['worker_init'],
                channels=[SSHChannel(hostname=m,
                                     username=user_opts['adhoc']['username'],
                                     script_dir=user_opts['adhoc']['script_dir'],
                ) for m in user_opts['adhoc']['remote_hostnames']]
            )
        )
    ],
)
