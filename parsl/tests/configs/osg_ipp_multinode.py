from parsl.executors.ipp_controller import Controller
from parsl.channels.ssh.ssh import SSHChannel
from parsl.providers.condor.condor import CondorProvider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.utils import get_rundir

# If you are a developer running tests, make sure to update parsl/tests/configs/user_opts.py
# If you are a user copying-and-pasting this as an example, make sure to either
#       1) create a local `user_opts.py`, or
#       2) delete the user_opts import below and replace all appearances of `user_opts` with the literal value
#          (i.e., user_opts['swan']['username'] -> 'your_username')
from .user_opts import user_opts

config = Config(
    executors=[
        IPyParallelExecutor(
            label='osg_remote_ipp',
            provider=CondorProvider(
                channel=SSHChannel(
                    hostname='login.osgconnect.net',
                    username=user_opts['osg']['username'],
                    script_dir=user_opts['osg']['script_dir']
                ),
                nodes_per_block=1,
                init_blocks=4,
                max_blocks=4,
                scheduler_options='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                worker_init=user_opts['osg']['worker_init'],
                walltime="01:00:00"
            ),
            controller=Controller(public_ip=user_opts['public_ip'])
        )
    ],
    run_dir=get_rundir()
)
