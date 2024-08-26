import os

from parsl.config import Config
from parsl.executors import GlobusComputeExecutor


def fresh_config():

    endpoint_id = os.environ.get("GLOBUS_COMPUTE_ENDPOINT",
                                 "4b116d3c-1703-4f8f-9f6f-39921e5864df")
    return Config(
        executors=[
            GlobusComputeExecutor(
                label="globus_compute",
                endpoint_id=endpoint_id
            )
        ]
    )
