from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(
            label='local_threads_checkpoint',
        )
    ],
)
