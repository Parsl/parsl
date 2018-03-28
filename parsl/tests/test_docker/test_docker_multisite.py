"""Testing dockerized apps
"""
# import parsl
from parsl import *
# parsl.set_stream_logger()
import argparse
import random
import time

# Import and launch the MultiSite config
from localDockerIPP import localDockerMulti as config
dfk = DataFlowKernel(config=config)


@App('python', dfk, sites=['pool_app1'], cache=True)
def app_1(data):
    import app1
    return app1.predict(data)


@App('python', dfk, sites=['pool_app2'], cache=True)
def app_2(data):
    import app2
    return app2.predict(data)


def average(l):
    return sum(l) / len(l)


def test_simple(n=10):

    a1 = app_1([1, 2, 3])
    a2 = app_2([1, 2, 3])

    print("Priming")
    print("App1 results : ", a1.result())
    print("App2 results : ", a2.result())

    rands = list(range(1, 100))
    app1_rtts = []
    for i in range(0, 100):
        start = time.time()
        random.shuffle(rands)
        x = app_1(rands[0:3])
        x.result()
        rtt = time.time() - start
        app1_rtts.append(rtt)

    rands = list(range(1, 100))
    app2_rtts = []
    for i in range(0, 100):
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

    x = test_simple(int(args.count))
    # x = test_parallel_for(int(args.count))

    # x = test_stdout()
    # raise_error(0)
