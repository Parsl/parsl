from parsl.providers import LocalProvider
from parsl.channels import LocalChannel

from parsl.config import Config
from parsl.executors.mpix import MPIExecutor

config = Config(
    executors=[
        MPIExecutor(
            label="local_ipp",
            jobs_q_url="tcp://127.0.0.1:50005",
            results_q_url="tcp://127.0.0.1:50006",
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                tasks_per_node=3,
            )
        )
    ],
    strategy=None,
)
