from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider

from parsl.config import Config
from parsl.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            label='midway_htex_remote',
            max_workers=4,
            address="128.135.123.206",
            interchange_address="swift.rcc.uchicago.edu",
            worker_debug=True,
            provider=SlurmProvider(
                'westmere',
                channel=SSHChannel(
                    hostname='swift.rcc.uchicago.edu',
                    username='yadunand',
                    script_dir='/scratch/midway2/yadunand/'
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
