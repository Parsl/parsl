import sys
import json


class RPEX_ResourceConfig:
    def __init__(self):
        self.cores_per_node = 4
        self.gpus_per_node = 0

        self.n_masters = 1
        self.n_workers = 1
        self.masters_per_node = 1
        self.nodes_per_worker = 1

        python_v = '{0}.{1}'.format(sys.version_info[0],
                                    sys.version_info[1])

        self.pilot_env = {
            "type": "virtualenv",
            "version": python_v,
            "setup": ["mpi4py", "radical.pilot"]
        }

        self.master_descr = {
            "mode": "raptor.master",
            "named_env": "ve_rpex",
            "executable": "python3 rpex_master.py",
            "cpu_processes": 1
        }

        self.worker_descr = {
            "mode": "raptor.worker",
            "named_env": "ve_rpex",
            "worker_class": "RPEX_Worker",
            "worker_file": "./rpex_worker.py"
        }

    def get_cfg_file(self):
        # Convert the class instance to a cfg file.
        config_path = 'rpex.cfg'
        with open('rpex.cfg', 'w') as f:
            json.dump(self.__dict__, f, indent=4)
        return config_path
