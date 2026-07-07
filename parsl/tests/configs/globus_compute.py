import os

from globus_compute_sdk import Executor

from parsl.config import Config
from parsl.executors import GlobusComputeExecutor


def fresh_config():

    endpoint_id = os.environ["GLOBUS_COMPUTE_ENDPOINT"]

    return Config(
        executors=[
            GlobusComputeExecutor(
                executor=Executor(endpoint_id=endpoint_id),
                label="globus_compute",
            )
        ]
    )
