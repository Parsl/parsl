from parsl.providers import LocalProvider
from parsl.channels import SSHChannel
from parsl.executors import HighThroughputExecutor

from parsl.config import Config

username = "yadunand"
remotes = ['midway2-login1.rcc.uchicago.edu', 'midway2-login2.rcc.uchicago.edu']

config = Config(
    executors=[
        HighThroughputExecutor(
            label='remote_htex_{}'.format(m),
            cores_per_worker=4,
            worker_debug=False,
            address="128.135.112.73",
            provider=LocalProvider(
                init_blocks=1,
                nodes_per_block=1,
                parallelism=0.5,
                worker_init="source /scratch/midway2/yadunand/parsl_env_setup.sh",
                channel=SSHChannel(hostname=m,
                                   username=username,
                                   script_dir="/scratch/midway2/{}/parsl_tests/".format(username)
                )
            )
        ) for m in remotes
    ],
)
