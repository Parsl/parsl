from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import LowLatencyExecutor

config = Config(
    executors=[
        LowLatencyExecutor(
            label="llex_local",
            # worker_debug=True,
            workers_per_node=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    strategy=None,
)
