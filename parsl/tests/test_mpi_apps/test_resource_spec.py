import concurrent.futures
import contextlib
import logging
import multiprocessing
import os
import queue
import random
import time
import typing


import pytest
import unittest

import parsl
from parsl.app.app import python_app
from parsl.multiprocessing import SpawnContext
from parsl.tests.configs.htex_local import fresh_config
from typing import Dict
from parsl.executors.high_throughput.mpi_resource_management import (
    get_pbs_hosts_list,
    get_slurm_hosts_list,
    get_nodes_in_batchjob,
    MPIResourceTracker,
    identify_scheduler,
)
from parsl.executors.high_throughput.mpi_resource_management import MPIResourceUnavailable

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

    nodes_q = SpawnContext.Queue()
    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):
        tracker = MPIResourceTracker(nodes_q, "srun")
        nodes = get_pbs_hosts_list()
        n1 = tracker.get_nodes(4)
        n2 = tracker.get_nodes(2)
        n3 = tracker.get_nodes(2)
        logging.warning(f"{n1=} {n2=} {n3=} {nodes=}")
        assert set(nodes) == set(n1 + n2 + n3)
        assert nodes_q.empty()
        tracker.return_nodes(n1)
        tracker.return_nodes(n2)
        tracker.return_nodes(n3)


@pytest.mark.local
def test_randomized(rounds: int = 100):

    plan = [(8, ),
            (4, 4),
            (6, 2),
            (4, 2, 2),
            (6, 1, 1),
            (3, 3, 2),
            ]
    nodes_q = SpawnContext.Queue(maxsize=8)
    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):
        tracker = MPIResourceTracker(nodes_q, "srun")

        for iteration in range(rounds):
            current_plan = random.choice(plan)
            nodes_stack = []
            for req in current_plan:
                nodelist = tracker.get_nodes(req)
                nodes_stack.append(nodelist)
            assert nodes_q.empty()

            random.shuffle(nodes_stack)
            for item in nodes_stack:
                tracker.return_nodes(item)

            assert nodes_q.full()


@pytest.mark.local
def test_contention():

    nodes_q = SpawnContext.Queue(maxsize=8)
    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):
        tracker = MPIResourceTracker(nodes_q, "srun")

        nodes = tracker.get_nodes(6)
        logging.warning(f"Got {nodes=}")
        with pytest.raises(MPIResourceUnavailable) as exc:
            nodes = tracker.get_nodes(4)
        logging.warning(f"Got {exc=}")
        logging.warning(f"Got {exc.value=}")
        assert exc.value.requested == 4
        assert exc.value.available == 2
        tracker.return_nodes(nodes)
        nodes = tracker.get_nodes(4)
        assert len(nodes) == 4


def simulated_worker(task_queue: multiprocessing.Queue,
                     tracker: MPIResourceTracker,
                     result_queue: multiprocessing.Queue):
    while True:
        try:
            task = task_queue.get(timeout=0.2)
            task_id, nodecount, duration = task

            # Measure time to acquire nodes
            wait_s = time.time()
            nodes = tracker.get_nodes(nodecount)
            total_wait = round((time.time() - wait_s) * 1000, 2)

            # Sleep for the duration before returning nodes
            time.sleep(duration)
            tracker.return_nodes(nodes)

            result_queue.put((task_id, nodes, total_wait))
        except MPIResourceUnavailable:
            time.sleep(random.randint(1, 3))
            task_queue.put(task)

        except queue.Empty:
            logging.warning("Queue is empty. Exiting")
            break


@pytest.mark.local
def test_simulated_contention(rounds: int = 100):

    # node_choices = (8, 6, 4, 2, 1)
    node_choices = (4, 2, 1)
    sleep_choices = (0.0, 0.001, 0.002, 0.004)

    with mock_pbs_nodefile(nodefile="pbs_nodefile.8"):

        nodes_q = SpawnContext.Queue(maxsize=8)
        tracker = MPIResourceTracker(nodes_q, "srun")
        task_q = SpawnContext.Queue()
        result_q = SpawnContext.Queue()
        procs = []

        req_history = {}
        for task_id in range(rounds):
            task = (task_id, random.choice(node_choices), random.choice(sleep_choices))
            task_q.put(task)
            req_history[task_id] = task

        for i in range(4):
            proc = SpawnContext.Process(target=simulated_worker,
                                        args=(task_q, tracker, result_q))
            proc.start()
            procs.append(proc)

        for round in range(rounds):
            task_id, nodes, duration = result_q.get(timeout=10)
            assert task_id in req_history
            assert len(nodes) == req_history[task_id][1]
            del req_history[task_id]

        assert not req_history, "req_history should be empty after evicting results"
        [proc.join() for proc in procs]
        assert tracker.nodes_q.full()
