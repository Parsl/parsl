import logging
import multiprocessing
import os
import pickle
import queue
import subprocess
from enum import Enum
from typing import Dict, List

from parsl.multiprocessing import SpawnContext
from parsl.serialize import (pack_res_spec_apply_message,
                             unpack_res_spec_apply_message)

logger = logging.getLogger(__name__)


class Scheduler(Enum):
    Unknown = 0
    Slurm = 1
    PBS = 2
    Cobalt = 3


def get_slurm_hosts_list() -> List[str]:
    """Get list of slurm hosts from scontrol"""
    cmd = "scontrol show hostname $SLURM_NODELIST"
    b_output = subprocess.check_output(
        cmd, stderr=subprocess.STDOUT, shell=True
    )  # bytes
    output = b_output.decode().strip().split()
    return output


def get_pbs_hosts_list() -> List[str]:
    """Get list of PBS hosts from envvar: PBS_NODEFILE"""
    nodefile_name = os.environ["PBS_NODEFILE"]
    with open(nodefile_name) as f:
        return [line.strip() for line in f.readlines()]


def get_cobalt_hosts_list() -> List[str]:
    """Get list of COBALT hosts from envvar: COBALT_NODEFILE"""
    nodefile_name = os.environ["COBALT_NODEFILE"]
    with open(nodefile_name) as f:
        return [line.strip() for line in f.readlines()]


def get_nodes_in_batchjob(scheduler: Scheduler) -> List[str]:
    """Get nodelist from all supported schedulers"""
    nodelist = []
    if scheduler == Scheduler.Slurm:
        nodelist = get_slurm_hosts_list()
    elif scheduler == Scheduler.PBS:
        nodelist = get_pbs_hosts_list()
    elif scheduler == Scheduler.Cobalt:
        nodelist = get_cobalt_hosts_list()
    else:
        raise RuntimeError(f"mpi_mode does not support scheduler:{scheduler}")
    return nodelist


def identify_scheduler() -> Scheduler:
    """Use envvars to determine batch scheduler"""
    if os.environ.get("SLURM_NODELIST"):
        return Scheduler.Slurm
    elif os.environ.get("PBS_NODEFILE"):
        return Scheduler.PBS
    elif os.environ.get("COBALT_NODEFILE"):
        return Scheduler.Cobalt
    else:
        return Scheduler.Unknown


class MPINodesUnavailable(Exception):
    """Raised if there are no free nodes available for an MPI request"""

    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available

    def __str__(self):
        return f"MPINodesUnavailable(requested={self.requested} available={self.available})"


class TaskScheduler:
    """Default TaskScheduler that does no taskscheduling

    This class simply acts as an abstraction over the task_q and result_q
    that can be extended to implement more complex task scheduling logic
    """
    def __init__(
        self,
        pending_task_q: multiprocessing.Queue,
        pending_result_q: multiprocessing.Queue,
    ):
        self.pending_task_q = pending_task_q
        self.pending_result_q = pending_result_q

    def put_task(self, task) -> None:
        return self.pending_task_q.put(task)

    def get_result(self, block: bool, timeout: float):
        return self.pending_result_q.get(block, timeout=timeout)


class MPITaskScheduler(TaskScheduler):
    """Extends TaskScheduler to schedule MPI functions over provisioned nodes
    The MPITaskScheduler runs on a Manager on the lead node of a batch job, as
    such it is expected to control task placement over this single batch job.

    The MPITaskScheduler adds the following functionality:
    1) Determine list of nodes attached to current batch job
    2) put_task for execution onto workers:
        a) if resources are available attach resource list
        b) if unavailable place tasks into backlog
    3) get_result will fetch a result and relinquish nodes,
       and attempt to schedule tasks in backlog if any.
    """
    def __init__(
        self,
        pending_task_q: multiprocessing.Queue,
        pending_result_q: multiprocessing.Queue,
    ):
        super().__init__(pending_task_q, pending_result_q)
        self.scheduler = identify_scheduler()
        # PriorityQueue is threadsafe
        self._backlog_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._map_tasks_to_nodes: Dict[str, List[str]] = {}
        self.available_nodes = get_nodes_in_batchjob(self.scheduler)
        self._free_node_counter = SpawnContext.Value("i", len(self.available_nodes))
        # mp.Value has issues with mypy
        # issue https://github.com/python/typeshed/issues/8799
        # from mypy 0.981 onwards
        self.nodes_q: queue.Queue = queue.Queue()
        for node in self.available_nodes:
            self.nodes_q.put(node)

        logger.info(
            f"Starting MPITaskScheduler with {len(self.available_nodes)}"
        )

    def _get_nodes(self, num_nodes: int) -> List[str]:
        """Thread safe method to acquire num_nodes from free resources

        Raises: MPINodesUnavailable if there aren't enough resources
        Returns: List of nodenames:str
        """
        logger.debug(
            f"Requesting : {num_nodes=} we have {self._free_node_counter}"
        )
        acquired_nodes = []
        with self._free_node_counter.get_lock():
            if num_nodes <= self._free_node_counter.value:  # type: ignore[attr-defined]
                self._free_node_counter.value -= num_nodes  # type: ignore[attr-defined]
            else:
                raise MPINodesUnavailable(
                    requested=num_nodes, available=self._free_node_counter.value  # type: ignore[attr-defined]
                )

            for i in range(num_nodes):
                node = self.nodes_q.get()
                acquired_nodes.append(node)
        return acquired_nodes

    def _return_nodes(self, nodes: List[str]) -> None:
        """Threadsafe method to return a list of nodes"""
        for node in nodes:
            self.nodes_q.put(node)
        with self._free_node_counter.get_lock():
            self._free_node_counter.value += len(nodes)  # type: ignore[attr-defined]

    def put_task(self, task_package: dict):
        """Schedule task if resources are available otherwise backlog the task"""
        user_ns = locals()
        user_ns.update({"__builtins__": __builtins__})
        _f, _args, _kwargs, resource_spec = unpack_res_spec_apply_message(
            task_package["buffer"], user_ns, copy=False
        )

        nodes_needed = resource_spec.get("num_nodes")
        if nodes_needed:
            try:
                allocated_nodes = self._get_nodes(nodes_needed)
            except MPINodesUnavailable:
                logger.warning("Not enough resources, placing task into backlog")
                self._backlog_queue.put((nodes_needed, task_package))
                return
            else:
                resource_spec["MPI_NODELIST"] = ",".join(allocated_nodes)
                self._map_tasks_to_nodes[task_package["task_id"]] = allocated_nodes
                buffer = pack_res_spec_apply_message(_f, _args, _kwargs, resource_spec)
                task_package["buffer"] = buffer

        self.pending_task_q.put(task_package)

    def _schedule_backlog_tasks(self):
        """Attempt to schedule backlogged tasks"""
        try:
            _nodes_requested, task_package = self._backlog_queue.get(block=False)
            self.put_task(task_package)
        except queue.Empty:
            return
        else:
            # Keep attempting to schedule tasks till we are out of resources
            self._schedule_backlog_tasks()

    def get_result(self, block: bool, timeout: float):
        """Return result and relinquish provisioned nodes"""
        result_pkl = self.pending_result_q.get(block, timeout=timeout)
        result_dict = pickle.loads(result_pkl)
        if result_dict["type"] == "result":
            task_id = result_dict["task_id"]
            nodes_to_reallocate = self._map_tasks_to_nodes[task_id]
            self._return_nodes(nodes_to_reallocate)
            self._schedule_backlog_tasks()

        return result_pkl
