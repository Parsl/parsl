from libsubmit.providers import LocalProvider
from libsubmit.channels import LocalChannel

from parsl.config import Config
from parsl.executors.mpix import MPIExecutor

config = Config(
    executors=[
        MPIExecutor(
            label="local_ipp",
            # These urls here are *NOT* used
            jobs_q_url="tcp://127.0.0.1:50005",
            results_q_url="tcp://127.0.0.1:50006",
            # launch_cmd="mpiexec -np {tasks_per_node} /home/yadu/src/parsl/parsl/executors/mpix/fabric.py {debug}",
            # launch_cmd="./cleanup.sh ; mpiexec -np {tasks_per_node} /home/yadu/src/parsl/parsl/executors/mpix/fabric_threaded.py {debug}",
            launch_cmd="./cleanup.sh ; python3 fabric_single_node.py {debug} --task_url={task_url} --result_url={result_url}",
            # launch_cmd="sleep 600",
            # engine_debug=True,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
                tasks_per_node=3,
            )
        )
    ],
    strategy=None,
)
