from parsl.config import Config
from parsl.executors import WorkQueueExecutor

config = Config(
    executors=[WorkQueueExecutor(port=50055,
                                 project_name="WQexample",
                                 see_worker_output=True,
                                 source=True,
                                 # init_command='source /home/yadu/src/wq_parsl/setup_parsl_env.sh;
                                 # echo "Ran at $date" > /home/yadu/src/wq_parsl/parsl/tests/workqueue_tests/ran.log',
        )]
)
