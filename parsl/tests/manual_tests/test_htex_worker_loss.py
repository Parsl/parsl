import argparse
import time
import parsl
import pytest

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.launchers import SingleNodeLauncher
from parsl.config import Config
from parsl.executors import HighThroughputExecutor

try:
    from parsl.addresses import address_by_interface
    address = address_by_interface('enx00e112002c62')
except OSError:
    print("***************ERROR****************************")
    print("Interface doesn't exist, falling back to default, 127.0.0.1")
    print("***************ERROR****************************")
    address = '127.0.01'


config = Config(
    executors=[
        HighThroughputExecutor(
            poll_period=1,
            address=address,
            label="htex_local",
            worker_debug=True,
            cores_per_worker=1,
            heartbeat_period=1,
            heartbeat_threshold=2,
            worker_ports=(54759, 54330),
            suppress_failure=True,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=0,
                max_blocks=0,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option should in most cases be 1
                launcher=SingleNodeLauncher(),
            ),
        )
    ],
    strategy=None,
)


# config.executors[0].provider.tasks_per_node = 4
from parsl.app.app import python_app  # , bash_app


@python_app
def double(x):
    return x * 2


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


@pytest.mark.skip('not asserting anything')
def test_simple(n=2, dur=10):
    """ Tests whether the right exception is returned when a manager is lost.
    Check whether the error string reports manager loss on node X.

    """
    start = time.time()

    # Prime a worker
    double(5).result()

    # Launch the slow task to a failing node.
    x = platform(sleep=dur)

    time.sleep(0.1)

    dfk = parsl.dfk()
    job_ids = dfk.executors['htex_local'].provider.resources.keys()
    dfk.executors['htex_local'].provider.cancel(job_ids)
    print("Result : ", x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


@pytest.mark.skip('not asserting anything')
def test_manager_fail(n=2, dur=10):
    """ Test manager failure due to intermittent n/w loss.
    Run parsl on laptop, start the worker on a remote node.
    Once connected and task is executed, kill the network
    confirm that the interchange does not error out.
    """
    start = time.time()

    # Prime a worker
    double(5).result()

    # Launch the slow task to a failing node.
    x = platform(sleep=dur)
    x.result()

    # At this point we know that the worker is connected.
    # Now we need to kill the network to mimic a n/w failure.
    time.sleep(0.1)
    print('*' * 80)
    print("Manually kill the network to create a n/w failure")
    print("We should see suppression of the failure")
    print('*' * 80)

    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    dfk = parsl.dfk()
    for i in range(200):
        print(dfk.executors['htex_local'].connected_workers)
        time.sleep(1)

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

    parsl.load(config)

    # x = test_simple(int(args.count))
    # x = test_imports()
    # x = test_simple(int(args.count), int(args.sleep))
    x = test_manager_fail(int(args.count), int(args.sleep))
