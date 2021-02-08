from parsl.config import Config
from parsl.executors import WorkQueueExecutor

config = Config(
    executors=[
        WorkQueueExecutor(
            label="parsl_wq_example",
            port=9123,
            project_name="parsl_wq_example",
            shared_fs=False
        )
    ]
)
