from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor
import os
config = Config(
    executors=[
        HighThroughputExecutor(
            # poll_period=10,
            label="htex_local",
            worker_debug=True,
            # worker_mode="singularity_single_use",
            worker_mode="no_container",
            # We always want the container to be in the home dir.
            container_image=os.path.expanduser("~/sing-run.simg"),
            cores_per_worker=1,
            #max_workers=8,
            max_workers=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                # max_workers=1,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option should in most cases be 1
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    run_dir="/home/ubuntu/parsl/parsl/tests/manual_tests/runinfo/",
    strategy=None,
)
