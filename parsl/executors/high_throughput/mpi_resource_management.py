import subprocess
from enum import Enum
import os
import multiprocessing
from typing import List
import logging
from parsl.multiprocessing import SpawnContext
logger = logging.getLogger(__name__)


class Scheduler(Enum):
    Unknown = 0
    Slurm = 1
    PBS = 2
    Cobalt = 3


def get_slurm_hosts_list() -> List[str]:
    cmd = "scontrol show hostname $SLURM_NODELIST"
    b_output = subprocess.check_output(
        cmd, stderr=subprocess.STDOUT, shell=True
    )  # bytes
    output = b_output.decode().strip().split()
    return output


def get_pbs_hosts_list() -> List[str]:
    nodefile_name = os.environ["PBS_NODEFILE"]
    with open(nodefile_name) as f:
        return [line.strip() for line in f.readlines()]


def get_cobalt_hosts_list() -> List[str]:
    nodefile_name = os.environ["COBALT_NODEFILE"]
    with open(nodefile_name) as f:
        return [line.strip() for line in f.readlines()]


def get_nodes_in_batchjob(scheduler: Scheduler) -> List[str]:
    nodelist = []
    if scheduler == Scheduler.Slurm:
        nodelist = get_slurm_hosts_list()
    elif scheduler == Scheduler.PBS:
        nodelist = get_pbs_hosts_list()
    elif scheduler == Scheduler.Cobalt:
        nodelist = get_cobalt_hosts_list()
    return nodelist


def identify_scheduler() -> Scheduler:
    if os.environ.get("SLURM_NODELIST"):
        return Scheduler.Slurm
    elif os.environ.get("PBS_NODEFILE"):
        return Scheduler.PBS
    elif os.environ.get("COBALT_NODEFILE"):
        return Scheduler.Cobalt
    else:
        return Scheduler.Unknown


class MPIResourceUnavailable(Exception):
    """Raised if there are no free resources available for an MPI request"""
    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available

    def __str__(self):
        return f"MPIResourceUnavailable(requested={self.requested} available={self.available}"


class MPIResourceTracker:
    def __init__(
        self,
        nodes_q: multiprocessing.Queue,
        mpi_launcher: str,
    ):
        self.nodes_q = nodes_q
        self.mpi_launcher = mpi_launcher
        self.scheduler = identify_scheduler()
        self.available_nodes = get_nodes_in_batchjob(self.scheduler)
        self._free_node_counter = SpawnContext.Value("i", len(self.available_nodes))

        logger.debug(f"scheduler:{self.scheduler}, nodes:{self.available_nodes}")

        for node in self.available_nodes:
            self.nodes_q.put(node)

    def get_nodes(self, num_nodes: int) -> List[str]:
        logger.debug(f"requesting : {num_nodes=} we have {self._free_node_counter}")
        acquired_nodes = []
        with self._free_node_counter.get_lock():
            if num_nodes <= self._free_node_counter.value:
                self._free_node_counter.value -= num_nodes
            else:
                raise MPIResourceUnavailable(requested=num_nodes, available=self._free_node_counter.value)

            for i in range(num_nodes):
                node = self.nodes_q.get()
                acquired_nodes.append(node)

        return acquired_nodes

    def return_nodes(self, nodes: List[str]) -> None:
        for node in nodes:
            self.nodes_q.put(node)
        with self._free_node_counter.get_lock():
            self._free_node_counter.value += len(nodes)
