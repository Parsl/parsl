import logging
from typing import Dict, List, Tuple, Set

logger = logging.getLogger(__name__)

VALID_LAUNCHERS = ('srun',
                   'aprun',
                   'mpiexec')


class InvalidResourceSpecification(Exception):
    """Exception raised when Invalid keys are supplied via resource specification"""

    def __init__(self, invalid_keys: Set[str]):
        self.invalid_keys = invalid_keys

    def __str__(self):
        return f"Invalid resource specification options supplied: {self.invalid_keys}"


def validate_resource_spec(resource_spec: Dict[str, str]):
    """Basic validation of keys in the resource_spec

    Raises: InvalidResourceSpecification if the resource_spec
        is invalid (e.g, contains invalid keys)
    """
    user_keys = set(resource_spec.keys())
    legal_keys = set(("RANKS_PER_NODE",
                      "NUM_NODES",
                      "LAUNCHER_OPTIONS",
                      ))
    invalid_keys = user_keys - legal_keys
    if invalid_keys:
        raise InvalidResourceSpecification(invalid_keys)
    return


def compose_mpiexec_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:
    """Compose mpiexec launch command prefix"""

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
    """Compose srun launch command prefix"""

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
    """Compose aprun launch command prefix"""

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
    """Compose all launch command prefixes and set the default"""

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
        raise RuntimeError(f"Unknown mpi_launcher:{mpi_launcher}")

    return all_prefixes
