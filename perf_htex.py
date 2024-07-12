from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


from parsl.executors.high_throughput.executor import DEFAULT_LAUNCH_CMD

def fresh_config():
    return Config(
        initialize_logging=False,
        executors=[
            HighThroughputExecutor(

                # elixirchange needs no encryption
                encrypted=False,
                benc_interchange_cli="elixir",

                launch_cmd="fake_"+DEFAULT_LAUNCH_CMD,
                label="htex_local",
                worker_debug=False,
                # leave some cores free on my laptop for non-worker stuff
                max_workers=0,
                cores_per_worker=1,

                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=3,
                    max_blocks=3,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy='none',
    )
