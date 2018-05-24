from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

config = Config(executors=[IPyParallelExecutor()])
