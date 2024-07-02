from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.providers import AdHocProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='AdHoc',
                encrypted=True,
                provider=AdHocProvider(
                    channels=[LocalChannel(), LocalChannel()]
                )
            )
        ]
    )
