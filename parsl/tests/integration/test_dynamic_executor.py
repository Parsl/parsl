import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.app.app import python_app

dfk = parsl.load()

@python_app(executors=['threads'])
def sleeper(dur=1):
    import time
    time.sleep(dur)


@python_app(executors=['threads2'])
def cpu_stress(dur=2):
    import time
    s = 0
    start = time.time()
    for i in range(10**8):
        s += i
        if time.time() - start >= dur:
            break
    return s


def test_dynamic_executor():
    tasks = [sleeper() for i in range(10)]
    results = [i.result() for i in tasks]
    print("Done with initial test. The results are", results)

    # Here we add a new executor to an active DFK
    thread_executors = [ThreadPoolExecutor(
        label='threads2',
        max_threads=4)
    ]
    dfk.add_executors(executors=thread_executors)

    tasks = [cpu_stress() for i in range(10)]
    results = [i.result() for i in tasks]
    print("Successfully added thread executor and ran with it. The results are", results)
    print("Done testing")


if __name__ == "__main__":
    test_dynamic_executor()
