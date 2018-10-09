from parsl.providers import LocalProvider
from parsl.channels import LocalChannel

from parsl.config import Config
from parsl.executors.mpix import MPIExecutor

config = Config(
    executors=[
        MPIExecutor(
            label="MPIX_Local",

            # Set the public ip of the machine you are launching the workflow from. Default: 127.0.0.1 for local runs.
            # public_ip = "10.236.1.193"

            # Set the worker ports explicitly. This is useful for re-using workers between workflows.
            # worker_ports=(50078, 50079),

            # Set a port range from which ports should be picked.
            worker_port_range=(40010, 40020),

            # The fabric_threaded.py script launches the MPI version
            # launch_cmd="./cleanup.sh ; mpiexec -np 4 python3 fabric_threaded.py {debug} --task_url={task_url} --result_url={result_url}",

            # launch_cmd="./cleanup.sh ; fabric_single_node.py {debug} --task_url={task_url} --result_url={result_url} ",
            # launch_cmd="./cleanup.sh ",
            # launch_cmd="sleep 600",

            # Enable engine debug logging
            engine_debug=True,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                tasks_per_node=1,
            )
        )
    ],
    strategy=None,
)
