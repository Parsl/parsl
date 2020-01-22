from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor


def fresh_config():
    return Config(executors=[ThreadPoolExecutor()])


config = fresh_config()
