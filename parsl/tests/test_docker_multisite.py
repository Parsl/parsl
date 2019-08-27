"""Testing dockerized apps
"""
import argparse
import random
import shutil
import time

import pytest

import parsl
from parsl.providers import LocalProvider
from parsl.app.app import App
from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

local_config = Config(
    executors=[
        IPyParallelExecutor(
            label='pool_app1',
            provider=LocalProvider(init_blocks=1),
            container_image='app1_v0.1'
        ),
        IPyParallelExecutor(
            label='pool_app2',
            provider=LocalProvider(init_blocks=1),
            container_image='app2_v0.1'
        )
    ]
)


@App('python', executors=['pool_app1'], cache=True)
def app_1(data):
    import app1
    return app1.predict(data)


@App('python', executors=['pool_app2'], cache=True)
def app_2(data):
    import app2
    return app2.predict(data)


def average(l):
    return sum(l) / len(l)


@pytest.mark.skip('broken')
@pytest.mark.skipif(shutil.which('docker') is None, reason='docker not installed')
@pytest.mark.local
@pytest.mark.usefixtures('setup_docker')
def test_simple(n=2):

    a1 = app_1([1, 2, 3])
    a2 = app_2([1, 2, 3])

    print("Priming")
    print("App1 results: ", a1.result())
    print("App2 results: ", a2.result())

    rands = list(range(1, 100))
    app1_rtts = []
    for i in range(0, n):
        start = time.time()
        random.shuffle(rands)
        x = app_1(rands[0:3])
        x.result()
        rtt = time.time() - start
        app1_rtts.append(rtt)

    rands = list(range(1, 100))
    app2_rtts = []
    for i in range(0, n):
        start = time.time()
        random.shuffle(rands)
        x = app_2(rands[0:3])
        x.result()
        rtt = time.time() - start
        app2_rtts.append(rtt)

    rtt = app1_rtts
    min_rtt = min(rtt) * 1000
    max_rtt = max(rtt) * 1000
    avg_rtt = average(rtt) * 1000
    print("App1_RTT   |   Min:{0:0.3}ms Max:{1:0.3}ms Average:{2:0.3}ms".format(min_rtt,
                                                                                max_rtt,
                                                                                avg_rtt))

    rtt = app2_rtts
    min_rtt = min(rtt) * 1000
    max_rtt = max(rtt) * 1000
    avg_rtt = average(rtt) * 1000
    print("App2_RTT   |   Min:{0:0.3}ms Max:{1:0.3}ms Average:{2:0.3}ms".format(min_rtt,
                                                                                max_rtt,
                                                                                avg_rtt))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    # if args.debug:
    #    parsl.set_stream_logger()

    parsl.load(local_config)

    x = test_simple(int(args.count))
