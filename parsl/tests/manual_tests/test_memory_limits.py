import argparse
import multiprocessing

import psutil

import parsl
from parsl.app.app import python_app  # , bash_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider


@python_app
def double(x):
    return x * 2


def test_simple(mem_per_worker):

    config = Config(
        executors=[
            HighThroughputExecutor(
                poll_period=1,
                label="htex_local",
                worker_debug=True,
                mem_per_worker=mem_per_worker,
                cores_per_worker=0.1,
                suppress_failure=True,
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                    launcher=SingleNodeLauncher(),
                ),
            )
        ],
        strategy='none',
    )
    parsl.load(config)

    print("Configuration requests:")
    print("cores_per_worker: ", config.executors[0].cores_per_worker)
    print("mem_per_worker: ", config.executors[0].mem_per_worker)

    available_mem_on_node = round(psutil.virtual_memory().available / (2**30), 1)
    expected_workers = multiprocessing.cpu_count() / config.executors[0].cores_per_worker
    if mem_per_worker:
        expected_workers = int(available_mem_on_node / config.executors[0].mem_per_worker)

    print("Available memory: ", available_mem_on_node)
    print("Expected workers: ", expected_workers)
    # Prime a worker
    double(5).result()
    dfk = parsl.dfk()
    connected = dfk.executors['htex_local'].connected_workers
    print("Connected : ", connected)
    assert expected_workers == connected, "Expected {} workers, instead got {} workers".format(expected_workers,
                                                                                               connected)
    parsl.clear()
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sleep", default="4")
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    available_mem_on_node = round(psutil.virtual_memory().available / (2**30), 1)

    x = test_simple(None)
    x = test_simple(0.8)
    x = test_simple(1)
    x = test_simple(available_mem_on_node - 2)
