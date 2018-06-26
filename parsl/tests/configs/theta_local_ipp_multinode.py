from libsubmit.providers.cobalt.cobalt import Cobalt
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='theta_local_ipp_multinode',
            provider=Cobalt(
                walltime="00:30:00",
                nodes_per_block=8,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                launcher='aprun',
                overrides=user_opts['account']['overrides'],
                account=user_opts['theta']['account']
            )
        )

    ],
    run_dir=get_rundir(),
    controller=Controller(public_ip=user_opts['theta']['public_ip'])
)
