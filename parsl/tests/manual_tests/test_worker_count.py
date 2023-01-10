import argparse
import time
import math
import multiprocessing

import parsl

CORES = multiprocessing.cpu_count()
CORES_PER_WORKER = 1
EXPECTED_WORKERS = math.floor(CORES / CORES_PER_WORKER)


# from parsl.tests.configs.htex_local import config
from parsl.tests.manual_tests.htex_local import config

from parsl.executors import HighThroughputExecutor
assert isinstance(config.executors[0], HighThroughputExecutor)
config.executors[0].cores_per_worker = CORES_PER_WORKER
config.executors[0].provider.init_blocks = 1

# from htex_midway import config
# from htex_swan import config

local_config = config

from parsl.app.app import python_app  # , bash_app


@python_app
def slow_pid(sleep=1):
    import os
    import time
    time.sleep(sleep)
    return os.getppid(), os.getpid()


def test_worker(n=2, sleep=0):
    d = {}
    start = time.time()
    for i in range(0, n):
        d[i] = slow_pid(sleep=sleep)

    foo = [d[i].result() for i in d]
    manager_ids = set([f[0] for f in foo])
    worker_ids = set([f[1] for f in foo])

    print("Got workers : {}".format(worker_ids))
    assert len(manager_ids) == 1, "Expected only 1 manager id, got ids : {}".format(
        manager_ids)
    assert len(worker_ids) == EXPECTED_WORKERS, "Expected {} workers, instead got {}".format(EXPECTED_WORKERS,
                                                                                             len(worker_ids))

    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sleep", default="0")
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    parsl.load(local_config)

    x = test_worker(n=int(args.count), sleep=float(args.sleep))
