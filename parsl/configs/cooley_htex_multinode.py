from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.launchers import MpiRunLauncher
from parsl.providers import CobaltProvider


config = Config(
    executors=[
        HighThroughputExecutor(
            label="cooley_htex",
            worker_debug=False,
            cores_per_worker=1,
            address=address_by_hostname(),
            provider=CobaltProvider(
                queue='debug',
                account='YOUR_ACCOUNT',  # project name to submit the job
                launcher=MpiRunLauncher(),
                scheduler_options='',  # string to prepend to #COBALT blocks in the submit script to the scheduler
                worker_init='',  # command to run before starting a worker, such as 'source activate env'
                init_blocks=1,
                max_blocks=1,
                min_blocks=1,
                nodes_per_block=4,
                cmd_timeout=60,
                walltime='00:10:00',
            ),
        )
    ],

)
