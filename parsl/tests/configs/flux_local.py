from parsl.config import Config
from parsl.executors import FluxExecutor


def fresh_config():
    return Config(
        executors=[FluxExecutor()],
    )


config = fresh_config()
