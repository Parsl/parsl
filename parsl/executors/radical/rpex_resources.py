import json
import sys
from typing import List

_setup_paths: List[str] = []
try:
    import radical.pilot as rp
    import radical.utils as ru
except ImportError:
    pass


MPI = "mpi"
RP_ENV = "rp"
CLIENT = "client"
RPEX_ENV = "ve_rpex"
MPI_WORKER = "MPIWorker"
DEFAULT_WORKER = "DefaultWorker"


class ResourceConfig:
    """
    This ResourceConfig class is an abstraction of the resource
    configuration of the RAPTOR layer in the RADICAL-Pilot runtime system.

    This class sets up the default configuration values for the executor and
    allows the user to specify different resource requirements flexibly.

    For more information:
    https://radicalpilot.readthedocs.io/en/stable/tutorials/raptor.html

    Parameters
    ----------
    masters : int
        The number of masters to be deployed by RAPTOR.
        Default is 1.

    workers : int
        The number of workers to be deployed by RAPTOR.
        Default is 1.

    worker_gpus_per_node : int
        The number of GPUs a worker will operate on per node.
        Default is 0.

    worker_cores_per_node : int
        The number of CPU cores a worker will operate on per node.
        Default is 4.

    cores_per_master : int
        The number of cores a master will operate on per node.
        Default is 1.

    nodes_per_worker : int
        The number of nodes to be occupied by every worker.
        Default is 1.

    pilot_env_path : str
        The path to an exisitng pilot environment.
        Default is an empty string (RADICAL-Pilot will create one).

    pilot_env_name : str
        The name of the pilot environment.
        Default is "ve_rpex".

    pilot_env_pre_exec : list
        List of commands to be executed before starting the pilot environment.
        Default is an empty list.

    pilot_env_type : str
        The type of the pilot environment (e.g., 'venv', 'conda').
        Default is "venv".

    pilot_env_setup : list
        List of setup commands/packages for the pilot environment.
        Default is an empty list.

    python_v : str
        The Python version to be used in the pilot environment.
        Default is determined by the system's Python version.

    worker_type : str
        The type of worker(s) to be deployed by RAPTOR on the compute
        resources.
        Default is "DefaultWorker".
    """

    masters: int = 1
    workers: int = 1

    worker_gpus_per_node: int = 0
    worker_cores_per_node: int = 4

    cores_per_master: int = 1
    nodes_per_worker: int = 1

    pilot_env_mode: str = CLIENT
    pilot_env_path: str = ""
    pilot_env_type: str = "venv"
    pilot_env_name: str = RP_ENV
    pilot_env_pre_exec: List[str] = []
    pilot_env_setup: List[str] = _setup_paths

    python_v: str = f'{sys.version_info[0]}.{sys.version_info[1]}'
    worker_type: str = DEFAULT_WORKER

    def get_config(cls, path=None):

        # Default ENV mode for RP is to reuse
        # the client side. If this is not the case,
        # then RP will create a new env named ve_rpex
        # The user need to make sure that under:
        # $HOME/.radical/pilot/configs/*_resource.json
        # that virtenv_mode = local
        if cls.pilot_env_mode != CLIENT:
            cls.pilot_env_name = RPEX_ENV

        if MPI in cls.worker_type.lower() and \
           "mpi4py" not in cls.pilot_env_setup:
            cls.pilot_env_setup.append("mpi4py")

        cfg = {
            'n_masters': cls.masters,
            'n_workers': cls.workers,
            'worker_type': cls.worker_type,
            'gpus_per_node': cls.worker_gpus_per_node,
            'cores_per_node': cls.worker_cores_per_node,
            'cores_per_master': cls.cores_per_master,
            'nodes_per_worker': cls.nodes_per_worker,

            'pilot_env': {
                "version": cls.python_v,
                "name": cls.pilot_env_name,
                "path": cls.pilot_env_path,
                "type": cls.pilot_env_type,
                "setup": cls.pilot_env_setup,
                "pre_exec": cls.pilot_env_pre_exec
            },

            'pilot_env_mode': cls.pilot_env_mode,

            'master_descr': {
                "ranks": 1,
                "cores_per_rank": 1,
                "mode": rp.RAPTOR_MASTER,
                "named_env": cls.pilot_env_name,
            },

            'worker_descr': {
                "mode": rp.RAPTOR_WORKER,
                "named_env": cls.pilot_env_name,
                "raptor_file": "./rpex_worker.py",
                "raptor_class": cls.worker_type if
                cls.worker_type.lower() != MPI else MPI_WORKER,
                "ranks": cls.nodes_per_worker * cls.worker_cores_per_node,
                "gpus_per_rank": cls.nodes_per_worker * cls.worker_gpus_per_node,
            }}

        # Convert the class instance to a Json file or a Config dict.
        if path:
            config_path = 'rpex.cfg'
            config_path = path + '/' + config_path
            with open(config_path, 'w') as f:
                json.dump(cfg, f, indent=4)
        else:
            config_obj = ru.Config(from_dict=cfg)
            return config_obj
