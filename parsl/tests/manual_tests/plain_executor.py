import time
# from requests import get

import argparse
import parsl
# from parsl.config import Config
# from parsl.app.app import python_app
# from parsl.executors import HighThroughputExecutor
# from parsl.providers.slurm.slurm import SlurmProvider
# from parsl.launchers import SimpleLauncher


def sleep(seconds):
    import time
    time.sleep(seconds)


def double(x):
    return x * 2


def call_sleep(size):
    print("Priming ....")
    start = time.time()
    primers = [double(i) for i in range(0, 2)]
    [p.result() for p in primers]
    delta = time.time() - start
    print("Priming done in {:10.4f} s".format(delta))

    start = time.time()
    tasks = [sleep(0) for _ in range(0, size)]
    for task in tasks:
        task.result()
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(args.count, delta))
    print("Throughput : {:8.3f} Tasks/s".format(int(args.count) / delta))


def call_double(size, executor):
    print("Priming ....")
    start = time.time()

    primers = [executor.submit(double, i) for i in range(0, 2)]
    print("Got results : ", [p.result() for p in primers])

    delta = time.time() - start
    print("Priming done in {:10.4f} s".format(delta))

    print("Launching tasks: {}".format(size))
    start = time.time()
    tasks = [executor.submit(double, i) for i in range(0, size)]
    for task in tasks:
        task.result()
    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(args.count, delta))
    print("Throughput : {:8.3f} Tasks/s".format(int(args.count) / delta))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="1000",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")

    args = parser.parse_args()

    parsl.set_stream_logger()
    # from htex_local import config
    from llex_local import config

    # config.executors[0].worker_debug = True
    dfk = parsl.load(config)

    executor = dfk.executors["llex_local"]
    # executor = dfk.executors["llex_local"]

    call_double(int(args.count), executor)
