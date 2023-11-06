from parsl.config import Config
from parsl.executors import WorkQueueExecutor
from parsl.providers import LocalProvider

config = Config(
    executors=[WorkQueueExecutor(port=50055,
                                 source=True,
                                 provider=LocalProvider(),
                                 # init_command='source /home/yadu/src/wq_parsl/setup_parsl_env.sh;
                                 # echo "Ran at $date" > /home/yadu/src/wq_parsl/parsl/tests/workqueue_tests/ran.log',
                                 )]
)
