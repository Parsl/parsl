from parsl.config import Config
from parsl.executors.taskvine import TaskVineExecutor
from parsl.executors.taskvine import TaskVineManagerConfig
from parsl.providers import CondorProvider

config = Config(
    executors=[TaskVineExecutor(manager_config=TaskVineManagerConfig(port=50055),
                                provider=CondorProvider(),
        )]
)
