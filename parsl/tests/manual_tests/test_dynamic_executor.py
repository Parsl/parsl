import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import LocalProvider
from parsl.app.app import python_app
from parsl.launchers import SrunLauncher
from parsl.providers.slurm.slurm import SlurmProvider


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


def local_setup():
    global dfk
    dfk = parsl.load(config)


def local_teardown():
    parsl.clear()


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


@python_app(executors=['midway_htex'])
def add(n):
    s = 0
    for i in range(n):
        s += i
    return s


if __name__ == "__main__":

    tasks = [sleeper() for i in range(5)]
    results = [i.result() for i in tasks]
    print("Done with initial test. The results are ", results)

    thread_executors = [ThreadPoolExecutor(
        label='threads',
        max_threads=4)
    ]
    dfk.add_executors(executors=thread_executors)
    tasks = [cpu_stress() for i in range(10)]
    results = [i.result() for i in tasks]
    print("Successfully added thread executor and ran with it. The results are ", results)

    htex_executors = [
        HighThroughputExecutor(
            label="midway_htex",
            # worker_debug=True,
            cores_per_worker=1,
            provider=SlurmProvider(
                'broadwl',
                launcher=SrunLauncher(),
                scheduler_options='#SBATCH --exclusive',
                worker_init='source activate dask',
                init_blocks=1,
                max_blocks=1,
                min_blocks=1,
                nodes_per_block=2,
                walltime='00:10:00',
            ),
        )
    ]
    dfk.add_executors(executors=htex_executors)
    tasks = [add(i) for i in range(60)]
    results = [i.result() for i in tasks]
    print("Successfully added midway_htex executor and ran with it. The results are ", results)
    print("Done testing")
