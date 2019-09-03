from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir


def fresh_config():
    return Config(
        executors=[
            ThreadPoolExecutor(
                label='local_threads_checkpoint',
            )
        ],
        run_dir=get_rundir(),
    )


config = fresh_config()
