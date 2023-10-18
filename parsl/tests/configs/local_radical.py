import os

from parsl.config import Config
from parsl.executors import RadicalPilotExecutor
from parsl.executors.radical.rpex_resources import ResourceConfig as rpex_cfg

bulk_mode = False
tests_path = os.path.abspath(os.path.dirname(__file__)).split('configs')[0]
radical_test_path = tests_path + 'test_radical'

# start the MPI workers
if os.environ.get("RPEX_MPI"):
    rpex_cfg.worker_type = "MPI"

# submit in bulks instead of stream
if os.environ.get("RPEX_BULK"):
    bulk_mode = True

rpex_cfg.pilot_env_pre_exec.append(f"export PYTHONPATH=$PYTHONPATH:{tests_path}:{radical_test_path}")
config = Config(
    executors=[RadicalPilotExecutor(rpex_cfg=rpex_cfg.get_cfg_file(),
                                    bulk_mode=bulk_mode, resource='local.localhost',
                                    access_schema='local', walltime=30, cores=4)])
