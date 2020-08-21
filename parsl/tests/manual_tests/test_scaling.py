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
            heartbeat_period=1,
            heartbeat_threshold=2,
            poll_period=1,
            label="htex_local",
            # worker_debug=True,
            max_workers=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=0,
                max_blocks=2,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option should in most cases be 1
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    max_idletime=1,
    strategy='htex_auto_scale',
)


@python_app
def sleeper(t):
    import random
    import time
    time.sleep(t)
    return random.randint(0, 10000)


@pytest.mark.local
def test_scale_out():
    dfk = parsl.dfk()

    # Since we have init_blocks = 0, at this point we should have 0 managers
    print("Executor : ", dfk.executors['htex_local'])
    print("Before")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: ", dfk.executors['htex_local'].outstanding)
    assert len(dfk.executors['htex_local'].connected_managers) == 0, "Expected 0 managers at start"
    fus = [sleeper(i) for i in (1, 1, 1, 1, 20)]

    fus[0].result()
    fus[1].result()

    # At this point, since we have 1 task already processed we should have atleast 1 manager
    assert len(dfk.executors['htex_local'].connected_managers) == 2, "Expected 2 managers once tasks are running"
    print("Between")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: ", dfk.executors['htex_local'].outstanding)

    fus[2].result()
    fus[3].result()
    time.sleep(10)
    [x.result() for x in fus]
    assert len(dfk.executors['htex_local'].connected_managers) == 1, "Expected 1 manager cleaning up"
    print("After")
    print("Managers   : ", dfk.executors['htex_local'].connected_managers)
    print("Outstanding: ", dfk.executors['htex_local'].outstanding)

    parsl


if __name__ == '__main__':
    # parsl.set_stream_logger()
    parsl.load(local_config)

    test_scale_out()
