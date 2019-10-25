from parsl.config import Config
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_query

config = Config(
    executors=[
        HighThroughputExecutor(
            label='OSG_HTEX',
            address=address_by_query(),
            max_workers=1,
            provider=CondorProvider(
                nodes_per_block=1,
                init_blocks=4,
                max_blocks=4,
                # This scheduler option string ensures that the compute nodes provisioned
                # will have modules
                scheduler_options='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                # Command to be run before starting a worker, such as:
                # 'module load Anaconda; source activate parsl_env'.
                worker_init='',
                walltime="00:20:00",
            ),
        )
    ]
)
