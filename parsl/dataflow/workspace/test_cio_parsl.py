# _*_ coding : utf-8  _*_

import parsl
import os
from parsl.app.app import python_app, bash_app
from parsl.configs.local_threads import config

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label="htex",
            #label="htex_local",
            # worker_debug=True,
            cores_per_worker=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option sho<
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    strategy='htex_totaltime',
    #strategy='simple',
    #strategy='htex_totaltime',
)


parsl.load(config)


@python_app
def hello ():
    return 'Hello World!'

print(hello().result())
