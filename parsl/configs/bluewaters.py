from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import AprunLauncher
from parsl.providers import TorqueProvider


config = Config(
    executors=[
        HighThroughputExecutor(
            label="bw_htex",
            cores_per_worker=1,
            worker_debug=False,
            provider=TorqueProvider(
                queue='normal',
                launcher=AprunLauncher(overrides="-b -- bwpy-environ --"),
                scheduler_options='',  # string to prepend to #SBATCH blocks in the submit script to the scheduler
                worker_init='',  # command to run before starting a worker, such as 'source activate env'
                init_blocks=1,
                max_blocks=1,
                min_blocks=1,
                nodes_per_block=2,
                walltime='00:10:00'
            ),
        )

    ],

)
