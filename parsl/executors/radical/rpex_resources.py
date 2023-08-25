import sys
import json
import radical.pilot as rp
import radical.utils as ru


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

    masters_per_node : int
        The number of masters to be placed on every node.
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
        Default setup includes "parsl", rp.sdist_path, and ru.sdist_path.

    python_v : str
        The Python version to be used in the pilot environment.
        Default is determined by the system's Python version.

    worker_type : str
        The type of worker(s) to be deployed by RAPTOR on the compute
        resources.
        Default is "DefaultWorker".
    """

    masters = 1
    workers = 1

    worker_gpus_per_node = 0
    worker_cores_per_node = 4

    masters_per_node = 1
    nodes_per_worker = 1

    pilot_env_path = ""
    pilot_env_name = "ve_rpex"
    pilot_env_pre_exec = []
    pilot_env_type = "venv"
    pilot_env_setup = ["parsl",
                       rp.sdist_path,
                       ru.sdist_path]

    python_v = f'{sys.version_info[0]}.{sys.version_info[1]}'
    worker_type = "DefaultWorker"

    @classmethod
    def get_cfg_file(cls):
        if "mpi" in cls.worker_type.lower() and "mpi4py" not in cls.pilot_env_setup:
            cls.pilot_env_setup.append("mpi4py")

        cfg = {
            'n_masters': cls.masters,
            'n_workers': cls.workers,
            'gpus_per_node': cls.worker_gpus_per_node,
            'cores_per_node': cls.worker_cores_per_node,
            'masters_per_node': cls.masters_per_node,
            'nodes_per_worker': cls.nodes_per_worker,

            'pilot_env': {
                "version": cls.python_v,
                "name": cls.pilot_env_name,
                "path": cls.pilot_env_path,
                "type": cls.pilot_env_type,
                "setup": cls.pilot_env_setup,
                "pre_exec": cls.pilot_env_pre_exec
            },

            'master_descr': {
                "mode": "raptor.master",
                "named_env": cls.pilot_env_name,
                "executable": "python3 rpex_master.py",
            },

            'worker_descr': {
                "mode": "raptor.worker",
                "named_env": cls.pilot_env_name,
                "raptor_class": cls.worker_type if cls.worker_type.lower() != "mpi" else "MPIWorker",
            }}

        # Convert the class instance to a cfg file.
        config_path = 'rpex.cfg'
        with open('rpex.cfg', 'w') as f:
            json.dump(cfg, f, indent=4)
        return config_path
