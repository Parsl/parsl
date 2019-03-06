import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.app.app import python_app

# parsl.set_stream_logger()

config = Config(
    executors=[
        HighThroughputExecutor(
            label='htex_local',
            cores_per_worker=1,
            max_workers=5,
            provider=LocalProvider(
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ],
    strategy=None,
)

dfk = parsl.load(config)


@python_app(executors=['htex_local'])
def sleeper(dur=1):
    import time
    time.sleep(dur)


@python_app(executors=['threads'])
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

    thread_executors = [ThreadPoolExecutor(
        label='threads',
        max_threads=4)
    ]
    dfk.add_executors(executors=thread_executors)
    tasks = [cpu_stress() for i in range(10)]
    results = [i.result() for i in tasks]
    print("Successfully added thread executor and ran with it. The results are", results)
    print("Done testing")


if __name__ == "__main__":
    test_dynamic_executor()
