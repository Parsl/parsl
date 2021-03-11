from parsl.config import Config
from parsl.executors.balsam.executor import BalsamExecutor

config = Config(executors=[BalsamExecutor()])
