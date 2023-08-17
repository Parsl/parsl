import sys
import json
import radical.pilot as rp
import radical.utils as ru


class RPEX_ResourceConfig:

    n_masters = 1
    n_workers = 1

    gpus_per_node = 0
    cores_per_node = 4

    masters_per_node = 1
    nodes_per_worker = 1

    pilot_env_path = ""
    pilot_env_pre_exec = []
    pilot_env_type = "venv"
    pilot_env_setup = ["parsl",
                       rp.sdist_path,
                       ru.sdist_path]

    python_v = f'{sys.version_info[0]}.{sys.version_info[1]}'
    rpex_worker = "DefaultWorker"

    @classmethod
    def get_cfg_file(cls):
        if 'MPI' in cls.rpex_worker and 'mpi4py' not in cls.pilot_env_setup:
            cls.pilot_env_setup.append('mpi4py')

        cfg = {
            'n_masters': cls.n_masters,
            'n_workers': cls.n_workers,
            'gpus_per_node': cls.gpus_per_node,
            'cores_per_node': cls.cores_per_node,
            'masters_per_node': cls.masters_per_node,
            'nodes_per_worker': cls.nodes_per_worker,

            'pilot_env': {
                "version": cls.python_v,
                "path": cls.pilot_env_path,
                "type": cls.pilot_env_type,
                "setup": cls.pilot_env_setup,
                "pre_exec": cls.pilot_env_pre_exec
            },

            'master_descr': {
                "mode": "raptor.master",
                "named_env": 've_rpex',
                "executable": "python3 rpex_master.py",
            },

            'worker_descr': {
                "mode": "raptor.worker",
                "named_env": 've_rpex',
                "raptor_class": cls.rpex_worker,
            }}

        # Convert the class instance to a cfg file.
        config_path = 'rpex.cfg'
        with open('rpex.cfg', 'w') as f:
            json.dump(cfg, f, indent=4)
        return config_path
