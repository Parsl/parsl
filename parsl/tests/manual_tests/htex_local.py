from parsl.config import Config
from parsl.executors import HighThroughputExecutor

# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider

config = Config(
    executors=[
        HighThroughputExecutor(
            poll_period=1,
            label="htex_local",
            # worker_debug=True,
            cores_per_worker=1,
            encrypted=True,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option should in most cases be 1
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    strategy='none',
)
