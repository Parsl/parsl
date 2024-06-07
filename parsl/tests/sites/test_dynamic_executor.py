import pytest

import parsl
from parsl.app.app import python_app
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import LocalProvider


@python_app(executors=['threads'])
def sleeper(dur=0):
    import time
    time.sleep(dur)


@python_app(executors=['threads2'])
def cpu_stress(dur=0.01):
    import time
    s = 0
    start = time.time()
    for i in range(10**8):
        s += i
        if time.time() - start >= dur:
            break
    return s


@python_app(executors=['htex_local'])
def add(dur=0.01):
    import time
    s = 0
    start = time.time()
    for i in range(10**8):
        s += i
        if time.time() - start >= dur:
            break
    return s


@pytest.mark.local
def test_dynamic_executor():
    dfk = parsl.load()
    tasks = [sleeper() for i in range(5)]
    results = [i.result() for i in tasks]
    print("Done with initial test. The results are", results)

    # Here we add a new executor to an active DFK
    thread_executors = [ThreadPoolExecutor(
        label='threads2',
        max_threads=4)
    ]
    dfk.add_executors(executors=thread_executors)
    tasks = [cpu_stress() for i in range(8)]
    results = [i.result() for i in tasks]
    print("Successfully added thread executor and ran with it. The results are", results)

    # We add a htex executor to an active DFK
    executors = [
        HighThroughputExecutor(
            label='htex_local',
            cores_per_worker=1,
            max_workers_per_node=5,
            encrypted=True,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ]
    dfk.add_executors(executors=executors)
    tasks = [add() for i in range(10)]
    results = [i.result() for i in tasks]
    print("Successfully added htex executor and ran with it. The results are", results)

    print("Done testing")

    dfk.cleanup()
