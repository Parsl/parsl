from libsubmit.channels.ssh_il.ssh_il import SSHInteractiveLoginChannel
from libsubmit.providers.cobalt.cobalt import Cobalt
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cooley_ssh_il_local_single_node',
            provider=Cobalt(
                channel=SSHInteractiveLoginChannel(
                    hostname='cooleylogin1.alcf.anl.gov',
                    username=user_opts['cooley']['username'],
                    script_dir="/home/{}/parsl_scripts/".format(user_opts['cooley']['username'])
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:05:00",
                overrides=user_opts['cooley']['overrides'],
                queue='debug',
                account=user_opts['cooley']['account']
            )
        )

    ],
    run_dir=get_rundir(),
)
