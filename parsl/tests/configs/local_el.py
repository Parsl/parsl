import os
from parsl.config import Config
from parsl.executors import EnsembleExecutor


def fresh_config():
    return Config(
        executors=[
            EnsembleExecutor(
                cpus=list(range(os.cpu_count())),
                task_executor_name="async_loky",
                master_logs=True,
                worker_logs=True,
            )
        ],
    )
