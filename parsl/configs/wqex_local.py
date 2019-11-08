from parsl.config import Config
from parsl.executors import WorkQueueExecutor

config = Config(
    executors=[
        WorkQueueExecutor(
            label="wqex_local",
            port=50055,
            project_name="WorkQueue Example",
            shared_fs=True,
            see_worker_output=True
        )
    ]
)
