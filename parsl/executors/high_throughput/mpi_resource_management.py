import subprocess
from enum import Enum
import os
import multiprocessing
from typing import List
import logging

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


class MPIResourceTracker:
    def __init__(
        self,
        nodes_q: multiprocessing.Queue,
        inflight_q: multiprocessing.Queue,
        mpi_launcher: str,
    ):
        self.nodes_q = nodes_q
        self.inflight_q = inflight_q
        self.mpi_launcher = mpi_launcher

        self.scheduler = identify_scheduler()
        self.available_nodes = get_nodes_in_batchjob(self.scheduler)

        logger.warning(f"YADU: scheduler:{self.scheduler}, nodes:{self.available_nodes}")

        for node in self.available_nodes:
            self.nodes_q.put(node)

    def get_nodes(self, num_nodes: int, owner_tag: str) -> List[str]:
        # On acquiring a node we place an ownership tag into the
        # inflight_q. This is done one at a time so that we do not
        # miss ownership info when we might block waiting for node
        acquired_nodes = []
        for i in range(num_nodes):
            node = self.nodes_q.get()
            self.inflight_q.put((owner_tag, node))
            acquired_nodes.append(node)

        return acquired_nodes

    def return_nodes(self, num_nodes: int, owner_tag: str):
        found_nodes = 0
        while found_nodes < num_nodes:
            tag, node = self.inflight_q.get()
            if tag == owner_tag:
                self.nodes_q.put(node)
                found_nodes += 1
            else:
                self.inflight_q.put((tag, node))
        return
