import sys
import json


class RPEX_ResourceConfig:

    cores_per_node = 4
    gpus_per_node = 0

    n_masters = 1
    n_workers = 1
    masters_per_node = 1
    nodes_per_worker = 1

    pilot_env_type = "virtualenv"
    pilot_env_path = ""
    pilot_env_setup = ["radical.pilot"]
    rpex_env = 've_rpex'
    python_v = '{0}.{1}'.format(sys.version_info[0],
                                sys.version_info[1])

    @classmethod
    def get_cfg_file(cls):
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
                "setup": cls.pilot_env_setup
            },

            'master_descr': {
                "cpu_processes": 1,
                "mode": "raptor.master",
                "named_env": cls.rpex_env,
                "executable": "python3 rpex_master.py",
            },

            'worker_descr': {
                "named_env": cls.rpex_env,
                "mode": "raptor.worker",
                "worker_class": "RPEX_Worker",
                "worker_file": "./rpex_worker.py"
            }}

        # Convert the class instance to a cfg file.
        config_path = 'rpex.cfg'
        with open('rpex.cfg', 'w') as f:
            json.dump(cfg, f, indent=4)
        return config_path
