from globus_compute_sdk import Executor

from parsl.config import Config
from parsl.executors import GlobusComputeExecutor
from parsl.usage_tracking.levels import LEVEL_1

# Public tutorial endpoint
tutorial_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'

config = Config(
    executors=[
        GlobusComputeExecutor(
            executor=Executor(endpoint_id=tutorial_endpoint),
            label="Tutorial_Endpoint_py3.11",
        )
    ],
    usage_tracking=LEVEL_1,
)
