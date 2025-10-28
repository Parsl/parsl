from globus_compute_sdk import Executor

from parsl.config import Config
from parsl.executors import GlobusComputeExecutor
from parsl.usage_tracking.levels import LEVEL_1

# Please start your own endpoint on perlmutter following instructions below to use this config:
# https://globus-compute.readthedocs.io/en/stable/endpoints/endpoint_examples.html#perlmutter-nersc
perlmutter_endpoint = 'YOUR_PERLMUTTER_ENDPOINT_UUID'

# Please start your own endpoint on expanse following instructions below to use this config:
# https://globus-compute.readthedocs.io/en/stable/endpoints/endpoint_examples.html#expanse-sdsc
expanse_endpoint = 'YOUR_EXPANSE_ENDPOINT_UUID'

config = Config(
    executors=[
        GlobusComputeExecutor(
            executor=Executor(endpoint_id=perlmutter_endpoint),
            label="Perlmutter",
        ),
        GlobusComputeExecutor(
            executor=Executor(endpoint_id=expanse_endpoint),
            label="Expanse",
        ),
    ],
    usage_tracking=LEVEL_1,
)
