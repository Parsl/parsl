from parsl.addresses import address_by_interface
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import MpiRunLauncher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.providers import PBSProProvider
from parsl.usage_tracking.levels import LEVEL_1

config = Config(
        executors=[
            HighThroughputExecutor(
                label="htex",
                heartbeat_period=15,
                heartbeat_threshold=120,
                worker_debug=True,
                max_workers_per_node=4,
                address=address_by_interface('ib0'),
                provider=PBSProProvider(
                    launcher=MpiRunLauncher(),
                    # PBS directives (header lines): for array jobs pass '-J' option
                    scheduler_options='#PBS -J 1-10',
                    # Command to be run before starting a worker, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    worker_init='',
                    # number of compute nodes allocated for each block
                    nodes_per_block=3,
                    min_blocks=3,
                    max_blocks=5,
                    cpus_per_node=24,
                    # medium queue has a max walltime of 24 hrs
                    walltime='24:00:00'
                ),
            ),
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_interface('ib0'),
            resource_monitoring_interval=10,
        ),
        strategy='simple',
        retries=3,
        app_cache=True,
        checkpoint_mode='task_exit',
        usage_tracking=LEVEL_1,
)
