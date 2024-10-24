import os

from parsl.config import Config
from parsl.executors import GlobusComputeExecutor


def fresh_config():

    endpoint_id = os.environ["GLOBUS_COMPUTE_ENDPOINT"]

    return Config(
        executors=[
            GlobusComputeExecutor(
                label="globus_compute",
                endpoint_id=endpoint_id
            )
        ]
    )
