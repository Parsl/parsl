from parsl.config import Config
from parsl.executors import TaskVineExecutor
from parsl.providers import CondorProvider

config = Config(
    executors=[TaskVineExecutor(port=50055,
                                source=True,
                                provider=CondorProvider(),
        )]
)
