import os

from parsl.config import Config
from parsl.executors import RadicalPilotExecutor
from parsl.executors.radical.rpex_resources import ResourceConfig as rpex_cfg

bulk_mode = False

# start the MPI workers
if os.environ.get("RPEX_MPI"):
    rpex_cfg.worker_type = "MPI"

# submit in bulks instead of stream
if os.environ.get("RPEX_BULK"):
    bulk_mode = True

# This is temporary; once everything is merged, we will use Parsl instead of
# this fork.
rpex_cfg.pilot_env_setup.append("pip install git+https://github.com/AymenFJA/parsl.git")

config = Config(
    executors=[RadicalPilotExecutor(rpex_cfg=rpex_cfg.get_cfg_file(),
                                    bulk_mode=bulk_mode, resource='local.localhost',
                                    access_schema='local', walltime=30, cores=4)])
