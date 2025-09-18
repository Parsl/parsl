import os
import pickle
from unittest import mock

import pytest

from parsl.executors.high_throughput.mpi_resource_management import (
    MPITaskScheduler,
    TaskScheduler,
)
from parsl.multiprocessing import SpawnContext
from parsl.serialize import pack_apply_message

mock_task_buffer = pack_apply_message("func", "args", "kwargs")


@pytest.fixture(autouse=True)
def set_pbs_nodefile_envvars():
    cwd = os.path.abspath(os.path.dirname(__file__))
    pbs_nodefile = os.path.join(cwd, "mocks", "pbs_nodefile.8")
    with mock.patch.dict(os.environ, {"PBS_NODEFILE": pbs_nodefile}):
        yield


@pytest.mark.local
def test_NoopScheduler():
    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = TaskScheduler(task_q, result_q)

    scheduler.put_task("TaskFoo")
    assert task_q.get() == "TaskFoo"

    result_q.put("Result1")
    assert scheduler.get_result(True, 1) == "Result1"


@pytest.mark.local
def test_MPISched_put_task():
    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = MPITaskScheduler(task_q, result_q)

    assert scheduler.available_nodes
    assert len(scheduler.available_nodes) == 8
    assert scheduler._free_node_counter.value == 8

    res_spec = {"num_nodes": 2, "ranks_per_node": 2}
    task_package = {"task_id": 1, "buffer": mock_task_buffer, "resource_spec": res_spec}
    scheduler.put_task(task_package)

    assert scheduler._free_node_counter.value == 6


@pytest.mark.local
def test_MPISched_get_result():
    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = MPITaskScheduler(task_q, result_q)

    assert scheduler.available_nodes
    assert len(scheduler.available_nodes) == 8
    assert scheduler._free_node_counter.value == 8

    nodes = [scheduler.nodes_q.get() for _ in range(4)]
    scheduler._free_node_counter.value = 4
    scheduler._map_tasks_to_nodes[1] = nodes

    result_package = pickle.dumps({"task_id": 1, "type": "result", "buffer": "Foo"})
    result_q.put(result_package)
    result_received = scheduler.get_result(block=True, timeout=1)
    assert result_received == result_package

    assert scheduler._free_node_counter.value == 8


@pytest.mark.local
def test_MPISched_roundtrip():
    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = MPITaskScheduler(task_q, result_q)

    assert scheduler.available_nodes
    assert len(scheduler.available_nodes) == 8

    for trip in range(1, 9):
        assert scheduler._free_node_counter.value == 8

        res_spec = {"num_nodes": trip, "ranks_per_node": 2}
        task_package = {"task_id": trip, "buffer": mock_task_buffer, "resource_spec": res_spec}
        scheduler.put_task(task_package)

        assert scheduler._free_node_counter.value == 8 - trip

        # Pop in a mock result
        result_pkl = pickle.dumps({"task_id": trip, "type": "result", "buffer": "RESULT BUF"})
        result_q.put(result_pkl)

        got_result = scheduler.get_result(True, 1)
        assert got_result == result_pkl


@pytest.mark.local
def test_MPISched_contention():
    """Second task has to wait for the first task due to insufficient resources"""
    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = MPITaskScheduler(task_q, result_q)

    assert scheduler.available_nodes
    assert len(scheduler.available_nodes) == 8

    assert scheduler._free_node_counter.value == 8

    rspec_1 = {"num_nodes": 8, "ranks_per_node": 2}
    task_package = {"task_id": 1, "buffer": mock_task_buffer, "resource_spec": rspec_1}
    scheduler.put_task(task_package)

    assert scheduler._free_node_counter.value == 0
    assert scheduler._backlog_queue.empty()

    rspec_2 = {"num_nodes": 8, "ranks_per_node": 2}
    task_package = {"task_id": 2, "buffer": mock_task_buffer, "resource_spec": rspec_2}
    scheduler.put_task(task_package)

    # Second task should now be in the backlog_queue
    assert not scheduler._backlog_queue.empty()

    # Confirm that the first task is available and has all 8 nodes provisioned
    task_on_worker_side = task_q.get()
    assert task_on_worker_side['task_id'] == 1
    assert len(rspec_1["MPI_NODELIST"].split(",")) == 8
    assert task_q.empty()  # Confirm that task 2 is not yet scheduled

    # Simulate worker returning result and the scheduler picking up result
    result_pkl = pickle.dumps({"task_id": 1, "type": "result", "buffer": "RESULT BUF"})
    result_q.put(result_pkl)
    got_result = scheduler.get_result(True, 1)
    assert got_result == result_pkl

    # Now task2 must be scheduled
    assert scheduler._backlog_queue.empty()

    # Pop in a mock result
    task_on_worker_side = task_q.get()
    assert task_on_worker_side['task_id'] == 2
    assert len(rspec_2["MPI_NODELIST"].split(",")) == 8


@pytest.mark.local
def test_hashable_backlog_queue():
    """Run multiple large tasks that to force entry into backlog_queue
    where queue.PriorityQueue expects hashability/comparability
    """

    task_q, result_q = SpawnContext.Queue(), SpawnContext.Queue()
    scheduler = MPITaskScheduler(task_q, result_q)

    assert scheduler.available_nodes
    assert len(scheduler.available_nodes) == 8

    assert scheduler._free_node_counter.value == 8

    for i in range(3):
        res_spec = {"num_nodes": 8, "ranks_per_node": 2}
        task_package = {"task_id": i, "buffer": mock_task_buffer, "resource_spec": res_spec}
        scheduler.put_task(task_package)
    assert scheduler._backlog_queue.qsize() == 2, "Expected 2 backlogged tasks"
