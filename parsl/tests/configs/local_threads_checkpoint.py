from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    return Config(
        executors=[
            ThreadPoolExecutor(
                label='local_threads_checkpoint',
            )
        ]
    )


config = fresh_config()
