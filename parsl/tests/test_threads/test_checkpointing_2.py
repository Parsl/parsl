import os
from parsl import *


def test_2():
    """From docs. Attempt loading checkpoint from previous run
    """

    from parsl.configs.local import localThreads as config
    last_runid = sorted(os.listdir('runinfo/'))[-1]
    last_checkpoint = os.path.abspath('runinfo/{0}/checkpoint'.format(last_runid))

    dfk = DataFlowKernel(config=config,
                         checkpointFiles=[last_checkpoint])

    # Test addition
    dfk.cleanup()
