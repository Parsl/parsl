import parsl
from parsl import python_app
parsl.set_stream_logger()

from parsl.providers import AdHocProvider
from parsl.channels import SSHChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor

remotes = ['midway2-login2.rcc.uchicago.edu', 'midway2-login1.rcc.uchicago.edu']

config = Config(
    executors=[
        HighThroughputExecutor(
            label='AdHoc',
            max_workers=2,
            worker_logdir_root="/scratch/midway2/yadunand/parsl_scripts",
            provider=AdHocProvider(
                worker_init="source /scratch/midway2/yadunand/parsl_env_setup.sh",
                channels=[SSHChannel(hostname=m,
                                     username="yadunand",
                                     script_dir="/scratch/midway2/yadunand/parsl_cluster")
                          for m in remotes]
            )
        )
    ]
)


@python_app
def platform(sleep=2, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


def test_raw_provider():

    parsl.load(config)

    x = [platform() for i in range(10)]
    print([i.result() for i in x])


if __name__ == "__main__":
    test_raw_provider()
