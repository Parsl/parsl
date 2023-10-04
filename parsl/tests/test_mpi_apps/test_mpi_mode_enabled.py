import logging
import typing
from typing import Dict
import pytest
import parsl
from parsl import python_app, bash_app
from parsl.tests.configs.htex_local import fresh_config

import os

EXECUTOR_LABEL = "MPI_TEST"


def local_setup():
    config = fresh_config()
    config.executors[0].label = EXECUTOR_LABEL
    config.executors[0].max_workers = 2
    config.executors[0].enable_mpi_mode = True

    cwd = os.path.abspath(os.path.dirname(__file__))
    pbs_nodefile = os.path.join(cwd, "mocks", "pbs_nodefile")

    config.executors[0].provider.worker_init = f"export PBS_NODEFILE={pbs_nodefile}"

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
        "NUM_NODES": 2,
        "RANKS_PER_NODE": 2,
    }

    future = get_env_vars(parsl_resource_specification=resource_spec)

    result = future.result()
    assert isinstance(result, Dict)
    logging.warning(f"Got table: {result}")
    assert "PARSL_MPI_PREFIX" in result
    assert "PARSL_MPIEXEC_PREFIX" in result
    assert result["PARSL_MPI_PREFIX"] == result["PARSL_MPIEXEC_PREFIX"]
    assert result["PARSL_NUM_NODES"] == str(resource_spec["NUM_NODES"])
    assert result["PARSL_RANKS_PER_NODE"] == str(resource_spec["RANKS_PER_NODE"])


@bash_app
def echo_launch_cmd(
    parsl_resource_specification: Dict,
    stdout=parsl.AUTO_LOGNAME,
    stderr=parsl.AUTO_LOGNAME,
):
    return 'echo "$PARSL_MPI_PREFIX hostname"'


@pytest.mark.local
def test_bash_default_prefix_set():
    """Confirm that resource_spec env vars are set while launch prefixes are not
    when enable_mpi_mode = False"""
    resource_spec = {
        "NUM_NODES": 2,
        "RANKS_PER_NODE": 2,
    }

    future = echo_launch_cmd(parsl_resource_specification=resource_spec)

    result = future.result()
    assert result == 0
    with open(future.stdout) as f:
        output = f.readlines()
        assert output[0].startswith("mpiexec")
        logging.warning(f"output : {output}")


@pytest.mark.local
def test_bash_multiple_set():
    """Confirm that multiple apps can run without blocking each other out
    when enable_mpi_mode = False"""
    resource_spec = {
        "NUM_NODES": 2,
        "RANKS_PER_NODE": 2,
    }
    futures = []
    for i in range(4):
        resource_spec["NUM_NODES"] = i + 1
        future = echo_launch_cmd(parsl_resource_specification=resource_spec)
        futures.append(future)

    for future in futures:
        result = future.result()
        assert result == 0
        with open(future.stdout) as f:
            output = f.readlines()
            assert output[0].startswith("mpiexec")


@bash_app
def bash_resource_spec(resource_specification: Dict, stdout=parsl.AUTO_LOGNAME):
    total_ranks = (
        resource_specification["RANKS_PER_NODE"] * resource_specification["NUM_NODES"]
    )
    return f'echo "{total_ranks}"'


@pytest.mark.local
def test_bash_app_using_resource_spec():
    resource_spec = {
        "NUM_NODES": 2,
        "RANKS_PER_NODE": 2,
    }
    future = bash_resource_spec(resource_spec)
    assert future.result() == 0
    with open(future.stdout) as f:
        output = f.readlines()
        total_ranks = resource_spec["NUM_NODES"] * resource_spec["RANKS_PER_NODE"]
        assert int(output[0].strip()) == total_ranks
