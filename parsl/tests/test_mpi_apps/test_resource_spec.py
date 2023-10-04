import contextlib
import logging
import multiprocessing
import os
import random
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
    MPIResourceTracker,
    identify_scheduler,
)

EXECUTOR_LABEL = "MPI_TEST"


def local_setup():
    config = fresh_config()
    config.executors[0].label = EXECUTOR_LABEL
    config.executors[0].max_workers = 1
    parsl.load(config)


def local_teardown():
    logging.warning("Exiting")
    parsl.dfk().cleanup()
    parsl.clear()


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
        "NUM_NODES": 4,
        "RANKS_PER_NODE": 2,
    }

    assert double(5).result() == 10

    future = get_env_vars(parsl_resource_specification=resource_spec)

    result = future.result()
    assert isinstance(result, Dict)
    assert result["PARSL_NUM_NODES"] == str(resource_spec["NUM_NODES"])
    assert result["PARSL_RANKS_PER_NODE"] == str(resource_spec["RANKS_PER_NODE"])


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
def test_node_tracking():

    nodes_q = multiprocessing.Queue()
    inflight_q = multiprocessing.Queue()
    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):
        tracker = MPIResourceTracker(nodes_q, inflight_q, "srun")
        nodes = get_pbs_hosts_list()
        n1 = tracker.get_nodes(4, "WORKER_1")
        n2 = tracker.get_nodes(2, "WORKER_2")
        n3 = tracker.get_nodes(2, "WORKER_3")
        logging.warning(f"{n1=} {n2=} {n3=} {nodes=}")
        assert set(nodes) == set(n1 + n2 + n3)
        assert nodes_q.empty()
        tracker.return_nodes(4, "WORKER_1")
        tracker.return_nodes(2, "WORKER_2")
        tracker.return_nodes(2, "WORKER_3")
        assert inflight_q.empty()


@pytest.mark.local
def test_randomized(rounds: int = 100):

    plan = [(8, ),
            (4, 4),
            (6, 2),
            (4, 2, 2),
            (6, 1, 1),
            (3, 3, 2),
            ]
    nodes_q = multiprocessing.Queue(maxsize=8)
    inflight_q = multiprocessing.Queue()
    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):
        tracker = MPIResourceTracker(nodes_q, inflight_q, "srun")

        for iteration in range(rounds):
            current_plan = random.choice(plan)
            req_stack = []
            for index, req in enumerate(current_plan):
                req = (req, f"WORKER_{index}")
                tracker.get_nodes(*req)
                req_stack.append(req)
            assert nodes_q.empty()

            for item in req_stack:
                tracker.return_nodes(*item)

            assert nodes_q.full()
            assert inflight_q.empty()
