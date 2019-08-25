from parsl.config import Config
from parsl.executors import WorkQueueExecutor

config = Config(executors=[WorkQueueExecutor(port=9000,
                                             see_worker_output=True,
                                             project_name="wq-testing")])
