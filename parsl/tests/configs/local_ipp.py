from parsl.providers import Local
from parsl.channels import LocalChannel

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

config = Config(
    executors=[
        IPyParallelExecutor(
            label="local_ipp",
            engine_dir='engines',
            provider=Local(
                channel=LocalChannel(),
                init_blocks=2,
                max_blocks=2,
            )
        )
    ]
)
