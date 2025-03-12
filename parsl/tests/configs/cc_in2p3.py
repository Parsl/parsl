from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import GridEngineProvider

from .user_opts import user_opts


def fresh_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label='cc_in2p3_htex',
                max_workers_per_node=1,
                encrypted=True,
                provider=GridEngineProvider(
                    nodes_per_block=2,
                    init_blocks=2,
                    max_blocks=2,
                    walltime="00:20:00",
                    scheduler_options=user_opts['cc_in2p3']['scheduler_options'],
                    worker_init=user_opts['cc_in2p3']['worker_init'],
                ),
            )
        ],
    )


config = fresh_config()
