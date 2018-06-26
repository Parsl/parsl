from parsl.executors.ipp_controller import Controller
from libsubmit.channels.local.local import LocalChannel
from libsubmit.providers.condor.condor import Condor
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.user_opts import user_opts
from parsl.tests.utils import get_rundir

config = Config(
    executors=[
        IPyParallelExecutor(
            label='osg_local_ipp',
            provider=Condor(
                channel=LocalChannel(
                    username=user_opts['osg']['username'],
                    script_dir=user_opts['osg']['script_dir']
                ),
                nodes_per_block=1,
                tasks_per_node=1,
                init_blocks=4,
                max_blocks=4,
                overrides='Requirements = OSGVO_OS_STRING == "RHEL 6" && Arch == "X86_64" &&  HAS_MODULES == True',
                worker_setup='module load python/3.5.2; python3 -m venv parsl_env; source parsl_env/bin/activate; pip3 install ipyparallel'
            )
        )

    ],
    controller=Controller(public_ip='192.170.227.195'),
    run_dir=get_rundir()
)
