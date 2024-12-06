from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_local",
            cores_per_worker=1,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ],
    usage_tracking=LEVEL_1,
)
