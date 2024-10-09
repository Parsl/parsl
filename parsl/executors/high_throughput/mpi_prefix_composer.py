import logging
from typing import Dict, List, Tuple

from parsl.executors.errors import InvalidResourceSpecification

logger = logging.getLogger(__name__)

VALID_LAUNCHERS = ('srun',
                   'aprun',
                   'mpiexec')


def validate_resource_spec(resource_spec: Dict[str, str]):
    """Basic validation of keys in the resource_spec

    Raises: InvalidResourceSpecification if the resource_spec
        is invalid (e.g, contains invalid keys)
    """
    user_keys = set(resource_spec.keys())

    # empty resource_spec when mpi_mode is set causes parsl to hang
    # ref issue #3427
    if len(user_keys) == 0:
        raise InvalidResourceSpecification(user_keys,
                                           'MPI mode requires optional parsl_resource_specification keyword argument to be configured')

    legal_keys = set(("ranks_per_node",
                      "num_nodes",
                      "num_ranks",
                      "launcher_options",
                      ))
    invalid_keys = user_keys - legal_keys
    if invalid_keys:
        raise InvalidResourceSpecification(invalid_keys)
    if "num_nodes" in resource_spec:
        if not resource_spec.get("num_ranks") and resource_spec.get("ranks_per_node"):
            resource_spec["num_ranks"] = str(int(resource_spec["num_nodes"]) * int(resource_spec["ranks_per_node"]))
        if not resource_spec.get("ranks_per_node") and resource_spec.get("num_ranks"):
            resource_spec["ranks_per_node"] = str(int(resource_spec["num_ranks"]) / int(resource_spec["num_nodes"]))
    return


def compose_mpiexec_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:
    """Compose mpiexec launch command prefix"""

    node_str = ",".join(node_hostnames)
    args = [
        "mpiexec",
        "-n",
        resource_spec.get("num_ranks"),
        "-ppn",
        resource_spec.get("ranks_per_node"),
        "-hosts",
        node_str,
        resource_spec.get("launcher_options", ""),
    ]
    prefix = " ".join(str(arg) for arg in args)
    return "PARSL_MPIEXEC_PREFIX", prefix


def compose_srun_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:
    """Compose srun launch command prefix"""

    num_nodes = str(len(node_hostnames))
    args = [
        "srun",
        "--ntasks",
        resource_spec.get("num_ranks"),
        "--ntasks-per-node",
        resource_spec.get("ranks_per_node"),
        "--nodelist",
        ",".join(node_hostnames),
        "--nodes",
        num_nodes,
        resource_spec.get("launcher_options", ""),
    ]

    prefix = " ".join(str(arg) for arg in args)
    return "PARSL_SRUN_PREFIX", prefix


def compose_aprun_launch_cmd(
    resource_spec: Dict, node_hostnames: List[str]
) -> Tuple[str, str]:
    """Compose aprun launch command prefix"""

    node_str = ",".join(node_hostnames)
    args = [
        "aprun",
        "-n",
        resource_spec.get("num_ranks"),
        "-N",
        resource_spec.get("ranks_per_node"),
        "-node-list",
        node_str,
        resource_spec.get("launcher_options", ""),
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
