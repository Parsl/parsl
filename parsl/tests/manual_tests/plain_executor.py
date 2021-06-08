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
    print(f"Priming done in {delta:10.4f} s")

    start = time.time()
    tasks = [sleep(0) for _ in range(0, size)]
    for task in tasks:
        task.result()

    delta = time.time() - start
    print(f"Time to complete {args.count} tasks: {delta:8.3f} s")
    print(f"Throughput : {int(args.count) / delta:8.3f} Tasks/s")


def call_double(size, executor):
    print("Priming ....")
    start = time.time()

    primers = [executor.submit(double, i) for i in range(0, 2)]
    print("Got results : ", [p.result() for p in primers])
    delta = time.time() - start
    print(f"Priming done in {delta:10.4f} s")

    print(f"Launching tasks: {size}")
    start = time.time()
    tasks = [executor.submit(double, i) for i in range(0, size)]

    for task in tasks:
        task.result()

    delta = time.time() - start

    print(f"Time to complete {args.count} tasks: {delta:8.3f} s")
    print(f"Throughput : {int(args.count) / delta:8.3f} Tasks/s")


def measure_latency(size, executor):
    print("Priming ....")
    start = time.time()

    primers = [executor.submit(double, i) for i in range(0, 2)]
    print("Got results : ", [p.result() for p in primers])
    delta = time.time() - start
    print(f"Priming done in {delta:10.4f} s")

    print(f"Launching tasks: {size}")

    start_all = time.time()
    tasks = []

    for i in range(size):
        start = time.time()
        fu = executor.submit(double, i)
        fu.result()
        delta = time.time() - start
        tasks.append(delta)

    delta_all = time.time() - start_all

    print(f"Time to complete {args.count} tasks: {delta_all:8.3f} s")
    print(f"Latency avg:{1000 * sum(tasks) / len(tasks):8.3f}ms  "
          f"min:{1000 * min(tasks):8.3f}ms  max:{1000 * max(tasks):8.3f}ms")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--count", default="1000",
                        help="Count of apps to launch")

    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")

    args = parser.parse_args()
    parsl.set_stream_logger()
    from htex_local import config

    # from llex_local import config
    dfk = parsl.load(config)
    executor = dfk.executors["htex_local"]
    # executor = dfk.executors["llex_local"]
    # config.executors[0].worker_debug = True
    # call_double(int(args.count), executor)
    measure_latency(int(args.count), executor)
