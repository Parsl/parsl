import contextlib
import logging
import os
import queue
import typing
import unittest
from typing import Dict
from unittest import mock

import pytest

from parsl.app.app import python_app
from parsl.executors.errors import InvalidResourceSpecification
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.executors.high_throughput.mpi_executor import MPIExecutor
from parsl.executors.high_throughput.mpi_resource_management import (
    get_nodes_in_batchjob,
    get_pbs_hosts_list,
    get_slurm_hosts_list,
    identify_scheduler,
)
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
from parsl.tests.configs.htex_local import fresh_config

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
        ({}, InvalidResourceSpecification),
    )
)
def test_mpi_resource_spec(resource_spec: Dict, exception):
    """Test validation of resource_specification in MPIExecutor"""

    mpi_ex = MPIExecutor(provider=LocalProvider(launcher=SimpleLauncher()))
    mpi_ex.outgoing_q = mock.Mock(spec=queue.Queue)

    if exception:
        with pytest.raises(exception):
            mpi_ex.validate_resource_spec(resource_spec)
    else:
        result = mpi_ex.validate_resource_spec(resource_spec)
        assert result is None


@pytest.mark.local
@pytest.mark.parametrize(
    "resource_spec",
    (
        {"num_nodes": 2, "ranks_per_node": 1},
        {"launcher_options": "--debug_foo"},
        {"BAD_OPT": 1},
    )
)
def test_mpi_resource_spec_passed_to_htex(resource_spec: dict):
    """HTEX should reject every resource_spec"""

    htex = HighThroughputExecutor()
    htex.outgoing_q = mock.Mock(spec=queue.Queue)

    with pytest.raises(InvalidResourceSpecification):
        htex.validate_resource_spec(resource_spec)
