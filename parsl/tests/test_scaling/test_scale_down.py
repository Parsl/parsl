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

local_config = Config(
    executors=[
        HighThroughputExecutor(
            heartbeat_period=2,
            heartbeat_threshold=6,
            poll_period=1,
            label="htex_local",
            # worker_debug=True,
            max_workers=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=0,
                max_blocks=5,
                min_blocks=2,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option should in most cases be 1
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    max_idletime=5,
    strategy='htex_auto_scale',
)


@python_app
def sleeper(t):
    import random
    import time
    time.sleep(t)
    return random.randint(0, 10000)


@pytest.mark.skip('fails 50% of time in CI - see issue #1885')
@pytest.mark.local
def test_scale_out():
    logger.info("Starting DFK")
    dfk = parsl.dfk()

    # Since we have init_blocks = 0, at this point we should have 0 managers
    logger.info(f"Executor: {dfk.executors['htex_local']}")
    logger.info(f"Managers: {dfk.executors['htex_local'].connected_managers}")
    logger.info(f"Outstanding: {dfk.executors['htex_local'].outstanding}")
    assert len(dfk.executors['htex_local'].connected_managers) == 0, "Expected 0 managers at start"

    fus = [sleeper(i) for i in [3, 3, 25, 25, 50]]

    logger.info("All apps invoked. Waiting for first two to finish.")

    for i in range(2):
        fus[i].result()

    # At this point, since we have 1 task already processed we should have at least 1 manager
    logger.info(f"Managers: {dfk.executors['htex_local'].connected_managers}")
    logger.info(f"Outstanding: {dfk.executors['htex_local'].outstanding}")
    assert len(dfk.executors['htex_local'].connected_managers) >= 1, "Expected at least one manager once tasks are running"

    logger.info("Sleeping 1s, assuming that this will be fast enough for full scale up")
    time.sleep(1)

    logger.info(f"Managers: {dfk.executors['htex_local'].connected_managers}")
    logger.info(f"Outstanding: {dfk.executors['htex_local'].outstanding}")
    assert len(dfk.executors['htex_local'].connected_managers) == 5, "Expected 5 managers 1 second after 2 tasks finished"

    logger.info("Waiting for all apps to complete")
    [x.result() for x in fus]
    logger.info("All apps completed")

    logger.info(f"Managers: {dfk.executors['htex_local'].connected_managers}")
    logger.info(f"Outstanding: {dfk.executors['htex_local'].outstanding}")

    # By this time, scale-down of some executors should have happened,
    # because the durations of the last two sleeper() invocations are
    # very long.
    assert len(dfk.executors['htex_local'].connected_managers) == 2, "Expected 2 managers when no tasks, lower bound by min_blocks"


if __name__ == '__main__':
    # parsl.set_stream_logger()
    parsl.load(local_config)

    test_scale_out()
