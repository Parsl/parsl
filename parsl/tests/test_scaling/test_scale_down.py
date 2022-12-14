import logging
import parsl
import pytest
import time
from parsl import python_app

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
# from parsl.launchers import SimpleLauncher
from parsl.launchers import SingleNodeLauncher

from parsl.config import Config
from parsl.executors import HighThroughputExecutor

logger = logging.getLogger(__name__)


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                heartbeat_period=2,
                heartbeat_threshold=6,
                poll_period=1,
                label="htex_local",
                max_workers=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=0,
                    max_blocks=5,
                    min_blocks=2,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        max_idletime=5,
        strategy='htex_auto_scale',
    )


@python_app
def sleeper(t):
    import time
    time.sleep(t)


# see issue #1885 for details of failures of this test.
# at the time of issue #1885 this test was failing frequently
# in CI.
@pytest.mark.local
def test_scale_out():
    logger.info("start")
    dfk = parsl.dfk()

    logger.info("initial asserts")
    assert len(dfk.executors['htex_local'].connected_managers) == 0, "Expected 0 managers at start"
    assert dfk.executors['htex_local'].outstanding == 0, "Expected 0 tasks at start"

    logger.info("launching tasks")
    fus = [sleeper(i) for i in [15 for x in range(0, 10)]]

    logger.info("waiting for warm up")
    time.sleep(15)

    logger.info("asserting 5 managers")
    assert len(dfk.executors['htex_local'].connected_managers) == 5, "Expected 5 managers after some time"

    logger.info("waiting for all futures to complete")
    [x.result() for x in fus]

    logger.info("asserting 0 outstanding tasks after completion")
    assert dfk.executors['htex_local'].outstanding == 0, "Expected 0 outstanding tasks after future completion"

    logger.info("waiting a while for scale down")
    time.sleep(25)

    logger.info("asserting 2 managers remain")
    assert len(dfk.executors['htex_local'].connected_managers) == 2, "Expected 2 managers when no tasks, lower bound by min_blocks"

    logger.info("test passed")
