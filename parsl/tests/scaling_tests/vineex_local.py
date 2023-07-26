from parsl.config import Config
from parsl.executors.taskvine import TaskVineExecutor
from parsl.executors.taskvine import TaskVineManagerConfig
from parsl.providers import LocalProvider

config = Config(
    executors=[TaskVineExecutor(label='VineExec',
                                use_factory=True,
                                manager_config=TaskVineManagerConfig(port=50055),
        )]
)
