from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    return Config(
        executors=[
            ThreadPoolExecutor(max_threads=4),
        ],
        app_cache=False
    )
