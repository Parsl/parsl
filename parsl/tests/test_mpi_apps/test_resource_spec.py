import contextlib
import logging
import os
import typing


import pytest
import unittest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config
from typing import Dict
from parsl.executors.high_throughput.mpi_resource_management import (
    get_pbs_hosts_list,
    get_slurm_hosts_list,
    get_nodes_in_batchjob,
    identify_scheduler,
)
from parsl.executors.high_throughput.mpi_prefix_composer import (
    validate_resource_spec,
    InvalidResourceSpecification
)

EXECUTOR_LABEL = "MPI_TEST"


def local_config():
    config = fresh_config()
    config.executors[0].label = EXECUTOR_LABEL
    config.executors[0].max_workers_per_node = 1
    return config


@python_app
def double(x, resource_spec=None):
    return x * 2


@python_app
def get_env_vars(parsl_resource_specification: Dict = {}) -> Dict:
    import os

    parsl_vars = {}
    for key in os.environ:
        if key.startswith("PARSL_"):
            parsl_vars[key] = os.environ[key]
    return parsl_vars


@pytest.mark.local
def test_resource_spec_env_vars():
    resource_spec = {
        "num_nodes": 4,
        "ranks_per_node": 2,
    }

    assert double(5).result() == 10

    future = get_env_vars(parsl_resource_specification=resource_spec)

    result = future.result()
    assert isinstance(result, Dict)
    assert result["PARSL_NUM_NODES"] == str(resource_spec["num_nodes"])
    assert result["PARSL_RANKS_PER_NODE"] == str(resource_spec["ranks_per_node"])


@pytest.mark.local
@unittest.mock.patch("subprocess.check_output", return_value=b"c203-031\nc203-032\n")
def test_slurm_mocked_mpi_fetch(subprocess_check):
    nodeinfo = get_slurm_hosts_list()
    assert isinstance(nodeinfo, list)
    assert len(nodeinfo) == 2


@contextlib.contextmanager
def add_to_path(path: os.PathLike) -> typing.Generator[None, None, None]:
    old_path = os.environ["PATH"]
    try:
        os.environ["PATH"] += str(path)
        yield
    finally:
        os.environ["PATH"] = old_path


@pytest.mark.local
@pytest.mark.skip
def test_slurm_mpi_fetch():
    logging.warning(f"Current pwd : {os.path.dirname(__file__)}")
    with add_to_path(os.path.dirname(__file__)):
        logging.warning(f"PATH: {os.environ['PATH']}")
        nodeinfo = get_slurm_hosts_list()
    logging.warning(f"Got : {nodeinfo}")


@contextlib.contextmanager
def mock_pbs_nodefile(nodefile: str = "pbs_nodefile") -> typing.Generator[None, None, None]:
    cwd = os.path.abspath(os.path.dirname(__file__))
    filename = os.path.join(cwd, "mocks", nodefile)
    try:
        os.environ["PBS_NODEFILE"] = filename
        yield
    finally:
        del os.environ["PBS_NODEFILE"]


@pytest.mark.local
def test_get_pbs_hosts_list():
    with mock_pbs_nodefile():
        nodelist = get_pbs_hosts_list()
        assert nodelist
        assert len(nodelist) == 4


@pytest.mark.local
def test_top_level():
    with mock_pbs_nodefile():
        scheduler = identify_scheduler()
        nodelist = get_nodes_in_batchjob(scheduler)
        assert len(nodelist) > 0


@pytest.mark.local
@pytest.mark.parametrize(
    "resource_spec, exception",
    (
        ({"num_nodes": 2, "ranks_per_node": 1}, None),
        ({"launcher_options": "--debug_foo"}, None),
        ({"num_nodes": 2, "BAD_OPT": 1}, InvalidResourceSpecification),
        ({}, None),
    )
)
def test_resource_spec(resource_spec: Dict, exception):
    if exception:
        with pytest.raises(exception):
            validate_resource_spec(resource_spec)
    else:
        result = validate_resource_spec(resource_spec)
        assert result is None
