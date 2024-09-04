from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers.ad_hoc.ad_hoc import DeprecatedAdHocProvider


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='AdHoc',
                encrypted=True,
                provider=DeprecatedAdHocProvider(
                    channels=[LocalChannel(), LocalChannel()]
                )
            )
        ]
    )
