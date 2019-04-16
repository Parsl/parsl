from parsl.executors.ipp_controller import Controller
from parsl.channels.ssh.ssh import SSHChannel
from parsl.providers.condor.condor import Condor
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='osg_remote_ipp',
            provider=Condor(
                channel=SSHChannel(
                    hostname='login.osgconnect.net',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/home/USERNAME/parsl_scripts',    # Please replace USERNAME with your username
                ),
                nodes_per_block=1,
                init_blocks=4,
                max_blocks=4,
                scheduler_options='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                worker_init='',     # Input your worker_init if needed
                walltime="01:00:00"
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )
    ],
)
