from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_checkpoint_dfk_exit',
        )
    ],
    checkpoint_mode='dfk_exit',
    run_dir=get_rundir(),
)
