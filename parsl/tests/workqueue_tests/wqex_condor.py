from parsl.config import Config
from parsl.executors import WorkQueueExecutor
from parsl.providers import CondorProvider

from shutil import which

path_to_worker = which('work_queue_worker')


# Fix:
# work_queue_worker should automatically be transfered without the need to
# specify transfer_input_files as an argument.
# The wrapper that executes the worker should do it using ./work_queue_worker.
# That PATH=. is a workaround not to break the local executor.

config = Config(
    executors=[WorkQueueExecutor(port=50055,
                                 project_name="WQexample",
                                 see_worker_output=True,
                                 source=True,
                                 provider=CondorProvider(transfer_input_files = [path_to_worker],
                                                         environment={'PATH': '.:/bin:/usr/bin'}),
                                 # init_command='source /home/yadu/src/wq_parsl/setup_parsl_env.sh;
                                 # echo "Ran at $date" > /home/yadu/src/wq_parsl/parsl/tests/workqueue_tests/ran.log',
        )]
)
