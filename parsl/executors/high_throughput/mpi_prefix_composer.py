import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

VALID_LAUNCHERS = ('srun',
                   'aprun',
                   'mpiexec')


def compose_mpiexec_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:
    node_str = ",".join(node_hostnames)
    num_ranks = resource_spec["RANKS_PER_NODE"] * len(node_hostnames)
    args = [
        "mpiexec",
        "-n",
        num_ranks,
        "-ppn",
        resource_spec.get("RANKS_PER_NODE"),
        "-hosts",
        node_str,
        resource_spec.get("LAUNCHER_OPTIONS", ""),
    ]
    prefix = " ".join(str(arg) for arg in args)
    return "PARSL_MPIEXEC_PREFIX", prefix


def compose_srun_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:

    num_ranks = resource_spec["RANKS_PER_NODE"] * len(node_hostnames)
    num_nodes = str(len(node_hostnames))
    args = [
        "srun",
        "--ntasks",
        num_ranks,
        "--ntasks-per-node",
        resource_spec.get("RANKS_PER_NODE"),
        "--nodelist",
        ",".join(node_hostnames),
        "--nodes",
        num_nodes,
        resource_spec.get("LAUNCHER_OPTIONS", ""),
    ]

    prefix = " ".join(str(arg) for arg in args)
    return "PARSL_SRUN_PREFIX", prefix


def compose_aprun_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:

    num_ranks = resource_spec["RANKS_PER_NODE"] * len(node_hostnames)
    node_str = ",".join(node_hostnames)
    args = [
        "aprun",
        "-n",
        num_ranks,
        "-N",
        resource_spec.get("RANKS_PER_NODE"),
        "-node-list",
        node_str,
        resource_spec.get("LAUNCHER_OPTIONS", ""),
    ]
    prefix = " ".join(str(arg) for arg in args)
    return "PARSL_APRUN_PREFIX", prefix


def compose_all(
    mpi_launcher: str, resource_spec: Dict, node_hostnames: List[str]
) -> Dict[str, str]:
    all_prefixes = {}
    composers = [
        compose_aprun_launch_cmd,
        compose_srun_launch_cmd,
        compose_mpiexec_launch_cmd,
    ]
    for composer in composers:
        try:
            key, prefix = composer(resource_spec, node_hostnames)
            all_prefixes[key] = prefix
        except Exception:
            logging.exception(
                f"Failed to compose launch prefix with {composer} from {resource_spec}"
            )
            pass

    if mpi_launcher == "srun":
        all_prefixes["PARSL_MPI_PREFIX"] = all_prefixes["PARSL_SRUN_PREFIX"]
    elif mpi_launcher == "aprun":
        all_prefixes["PARSL_MPI_PREFIX"] = all_prefixes["PARSL_APRUN_PREFIX"]
    elif mpi_launcher == "mpiexec":
        all_prefixes["PARSL_MPI_PREFIX"] = all_prefixes["PARSL_MPIEXEC_PREFIX"]
    else:
        # When unsure, we choose mpiexec
        all_prefixes["PARSL_MPI_PREFIX"] = all_prefixes["PARSL_MPIEXEC_PREFIX"]

    return all_prefixes
