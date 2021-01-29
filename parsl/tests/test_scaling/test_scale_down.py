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
    dfk = parsl.dfk()

    # Since we have init_blocks = 0, at this point we should have 0 managers
    print("Executor : ", dfk.executors['htex_local'])
    print("Before")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 0, "Expected 0 managers at start"

    fus = [sleeper(i) for i in [3, 3, 25, 25, 50]]

    for i in range(2):
        fus[i].result()

    # At this point, since we have 1 task already processed we should have atleast 1 manager
    print("Between")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 5, "Expected 5 managers once tasks are running"

    time.sleep(1)
    print("running")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 5, "Expected 5 managers 3 seconds after 2 tasks finished"

    time.sleep(21)
    print("Middle")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 3, "Expected 3 managers before cleaning up"

    for i in range(2, 4):
        fus[i].result()
    time.sleep(21)
    print("Finalizing result")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 2, "Expected 2 managers before finishing, lower bound by min_blocks"

    [x.result() for x in fus]
    print("Cleaning")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: \n", dfk.executors['htex_local'].outstanding)
    time.sleep(21)
    assert len(dfk.executors['htex_local'].connected_managers) == 2, "Expected 2 managers when no tasks, lower bound by min_blocks"


if __name__ == '__main__':
    # parsl.set_stream_logger()
    parsl.load(local_config)

    test_scale_out()
