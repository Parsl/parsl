from parsl.executors.ipp_controller import Controller
from libsubmit.channels.ssh.ssh import SSHChannel
from libsubmit.providers.condor.condor import Condor
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='osg_remote_ipp',
            provider=Condor(
                channel=SSHChannel(
                    hostname='login.osgconnect.net',
                    username=user_opts['osg']['username'],
                    script_dir=user_opts['osg']['script_dir']
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=4,
                max_blocks=4,
                overrides='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                worker_setup=user_opts['osg']['worker_setup'],
                walltime="01:00:00"
            ),
            controller=Controller(public_ip=user_opts['public_ip'])
        )
    ],
    run_dir=get_rundir()
)
