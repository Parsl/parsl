from typing import Dict
import pytest
import parsl
from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config

EXECUTOR_LABEL = "MPI_TEST"


def local_setup():
    config = fresh_config()
    config.executors[0].label = EXECUTOR_LABEL
    config.executors[0].max_workers_per_node = 1
    config.executors[0].enable_mpi_mode = False
    parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup()
    parsl.clear()


@python_app
def get_env_vars(parsl_resource_specification: Dict = {}) -> Dict:
    import os

    parsl_vars = {}
    for key in os.environ:
        if key.startswith("PARSL_"):
            parsl_vars[key] = os.environ[key]
    return parsl_vars


@pytest.mark.local
def test_only_resource_specs_set():
    """Confirm that resource_spec env vars are set while launch prefixes are not
    when enable_mpi_mode = False"""
    resource_spec = {
        "num_nodes": 4,
        "ranks_per_node": 2,
    }

    future = get_env_vars(parsl_resource_specification=resource_spec)

    result = future.result()
    assert isinstance(result, Dict)
    assert "PARSL_DEFAULT_PREFIX" not in result
    assert "PARSL_SRUN_PREFIX" not in result
    assert result["PARSL_NUM_NODES"] == str(resource_spec["num_nodes"])
    assert result["PARSL_RANKS_PER_NODE"] == str(resource_spec["ranks_per_node"])
