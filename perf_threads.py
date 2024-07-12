from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    return Config(
        initialize_logging=False,
        # 7 cores for the executor, 1 for Parsl submit side
        # although this code seems to behave mostly single threaded,
        # looking at CPU usage...
        executors=[ThreadPoolExecutor(max_threads=7)],
    )


config = fresh_config()
