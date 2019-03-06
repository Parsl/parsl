from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider

from parsl.config import Config
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='midway_htex_remote',
            max_workers=4,
            # address="128.135.123.206",    # Address on UC wired n/w
            address="34.204.113.50",        # Address of AWS host
            interchange_address="swift.rcc.uchicago.edu",  # Address at which workers can reach the ix
            interchange_port_range=(51000, 52000),         # Specify accessible ports
            # worker_debug=True,            # Be careful with this one, dumps a 1GB/few minutes
            provider=SlurmProvider(
                'sandyb',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username='yadunand',
                    script_dir='/scratch/midway2/yadunand/'  # Required. Logging dirs will be under this.
                ),
                init_blocks=1,
                min_blocks=1,
                max_blocks=2,
                nodes_per_block=1,
                parallelism=0.5,
                worker_init="cd /scratch/midway2/yadunand; source parsl_env_setup.sh"
            )
        )
    ]
)
