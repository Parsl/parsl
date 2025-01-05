from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider


def make_local_config(cores_per_worker: int = 1) -> Config:
    """Generate a configuration which runs all tasks on the local system

    Args:
        cores_per_worker: Number of cores to dedicate for each task
    Returns:
        Configuration object with the requested settings
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                cores_per_worker=cores_per_worker,
                cpu_affinity='block',
                provider=LocalProvider(),
            )
        ],
    )
