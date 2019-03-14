""" Use the following config with caution.
"""
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.tests.utils import get_rundir
from parsl.executors.ipp_controller import Controller

config = Config(
    executors=[
        IPyParallelExecutor(
            label='local_ipp_reuse',
            controller=Controller(reuse=True),
        ),
    ],
    run_dir=get_rundir(),
)
