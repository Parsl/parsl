# Untested
from parsl.channels import SSHInteractiveLoginChannel
from parsl.providers import CobaltProvider
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

# This is an example config, make sure to
#        replace the specific values below with the literal values
#          (e.g., 'USERNAME' -> 'your_username')

config = Config(
    executors=[
        IPyParallelExecutor(
            label='cooley_ssh_il_local_single_node',
            provider=CobaltProvider(
                channel=SSHInteractiveLoginChannel(
                    hostname='cooleylogin1.alcf.anl.gov',
                    username='USERNAME',     # Please replace USERNAME with your username
                    script_dir='/home/USERNAME/parsl_scripts/',    # Please replace USERNAME with your username
                ),
                nodes_per_block=1,
                init_blocks=1,
                max_blocks=1,
                walltime="00:05:00",
                scheduler_options='',     # Input your scheduler_options if needed
                worker_init='',     # Input your worker_init if needed
                queue='debug',
                account='ALCF_ALLOCATION',    # Please replace ALCF_ALLOCATION with your ALCF allocation
            ),
            controller=Controller(public_ip='PUBLIC_IP'),    # Please replace PUBLIC_IP with your public ip
        )

    ],
)
