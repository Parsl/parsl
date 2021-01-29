from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_checkpoint_periodic',
            max_threads=1
        )
    ],
    checkpoint_mode='periodic',
    checkpoint_period='00:00:05'
)
