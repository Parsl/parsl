from libsubmit.providers import CobaltProvider
from libsubmit.launchers import AprunLauncher

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='theta_local_ipp_multinode',
            provider=CobaltProvider(
                queue="debug-flat-quad",
                launcher=AprunLauncher(),
                walltime="00:30:00",
                nodes_per_block=2,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                overrides=user_opts['theta']['overrides'],
                account=user_opts['theta']['account'],
                cmd_timeout=60
            ),
            controller=Controller(public_ip=user_opts['public_ip'])
        )

    ],
    run_dir=get_rundir(),

)
