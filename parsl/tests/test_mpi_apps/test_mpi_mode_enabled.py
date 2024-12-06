import logging
import os
import random
from typing import Dict

import pytest

import parsl
from parsl import Config, bash_app, python_app
from parsl.executors import MPIExecutor
from parsl.executors.errors import InvalidResourceSpecification
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

EXECUTOR_LABEL = "MPI_TEST"


def local_setup():

    cwd = os.path.abspath(os.path.dirname(__file__))
    pbs_nodefile = os.path.join(cwd, "mocks", "pbs_nodefile")

    config = Config(
        executors=[
            MPIExecutor(
                label=EXECUTOR_LABEL,
                max_workers_per_block=2,
                mpi_launcher="mpiexec",
                provider=LocalProvider(
                    worker_init=f"export PBS_NODEFILE={pbs_nodefile}",
                    launcher=SimpleLauncher()
                )
            )
        ])

    parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup()


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
        "num_nodes": 2,
        "ranks_per_node": 2,
    }

    future = get_env_vars(parsl_resource_specification=resource_spec)

    result = future.result()
    assert isinstance(result, Dict)
    logging.warning(f"Got table: {result}")
    assert "PARSL_MPI_PREFIX" in result
    assert "PARSL_MPIEXEC_PREFIX" in result
    assert result["PARSL_MPI_PREFIX"] == result["PARSL_MPIEXEC_PREFIX"]
    assert result["PARSL_NUM_NODES"] == str(resource_spec["num_nodes"])
    assert result["PARSL_RANKS_PER_NODE"] == str(resource_spec["ranks_per_node"])
    assert result["PARSL_NUM_RANKS"] == str(resource_spec["ranks_per_node"] * resource_spec["num_nodes"])


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
        "num_nodes": 2,
        "ranks_per_node": 2,
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
        "num_nodes": 2,
        "num_ranks": 4,
    }
    futures = []
    for i in range(4):
        resource_spec["num_nodes"] = i + 1
        future = echo_launch_cmd(parsl_resource_specification=resource_spec)
        futures.append(future)

    for future in futures:
        result = future.result()
        assert result == 0
        with open(future.stdout) as f:
            output = f.readlines()
            assert output[0].startswith("mpiexec")


@bash_app
def bash_resource_spec(parsl_resource_specification=None, stdout=parsl.AUTO_LOGNAME):
    total_ranks = (
        parsl_resource_specification["ranks_per_node"] * parsl_resource_specification["num_nodes"]
    )
    return f'echo "{total_ranks}"'


@pytest.mark.local
def test_bash_app_using_resource_spec():
    resource_spec = {
        "num_nodes": 2,
        "ranks_per_node": 2,
    }
    future = bash_resource_spec(parsl_resource_specification=resource_spec)
    assert future.result() == 0
    with open(future.stdout) as f:
        output = f.readlines()
        total_ranks = resource_spec["num_nodes"] * resource_spec["ranks_per_node"]
        assert int(output[0].strip()) == total_ranks


@python_app
def mock_app(sleep_dur: float = 0.0, parsl_resource_specification: Dict = {}):
    import os
    import time
    time.sleep(sleep_dur)

    total_ranks = int(os.environ["PARSL_NUM_NODES"]) * int(os.environ["PARSL_RANKS_PER_NODE"])
    nodes = os.environ["PARSL_MPI_NODELIST"].split(',')

    return total_ranks, nodes


@pytest.mark.local
def test_simulated_load(rounds: int = 100):

    node_choices = (1, 2, 4)
    sleep_choices = (0, 0.01, 0.02, 0.04)
    ranks_per_node = (4, 8)

    futures = {}
    for i in range(rounds):
        resource_spec = {
            "num_nodes": random.choice(node_choices),
            "ranks_per_node": random.choice(ranks_per_node),
        }
        future = mock_app(sleep_dur=random.choice(sleep_choices),
                          parsl_resource_specification=resource_spec)
        futures[future] = resource_spec

    for future in futures:
        total_ranks, nodes = future.result(timeout=10)
        assert len(nodes) == futures[future]["num_nodes"]
        assert total_ranks == futures[future]["num_nodes"] * futures[future]["ranks_per_node"]


@pytest.mark.local
def test_missing_resource_spec():

    with pytest.raises(InvalidResourceSpecification):
        future = mock_app(sleep_dur=0.4)
        future.result(timeout=10)
