from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

config = Config(
    executors=[
        ThreadPoolExecutor(max_threads=4),
    ],
    app_cache=False
)
