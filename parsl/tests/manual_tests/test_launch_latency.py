import argparse
import time
import pytest

import parsl


# from parsl.tests.configs.local_threads import config
# from parsl.tests.configs.local_ipp import config
# from parsl.configs.htex_local import config
# from parsl.configs.exex_local import config
from llex_local import config


import pickle

from parsl.app.app import python_app  # , bash_app
# parsl.load(config)


@python_app
def double(x):
    return x * 2

@pytest.mark.skip('not asserting anything')
def test_launch_latency(n=2):
    d = {}

    launch_latency = []
    for i in range(0, n):
        start = time.time()
        d[i] = double(i)
        delta = time.time() - start
        launch_latency.append(delta * 1000)

    get_latency = []
    for i in d:
        start = time.time()
        d[i].result()
        delta = time.time() - start
        get_latency.append(delta * 1000)

    print("Launch latency min:{:=10.3f}ms min:{:=10.3f}ms avg:{:=10.3f}ms".format(min(launch_latency),
                                                                                  max(launch_latency),
                                                                                  sum(launch_latency) / len(launch_latency)))
    print("Get latency min:{:=10.3f}ms min:{:=10.3f}ms avg:{:=10.3f}ms".format(min(get_latency),
                                                                               max(get_latency),
                                                                               sum(get_latency) / len(get_latency)))
    print("[TEST STATMS] test_parallel_for [SUCCESS]")
    return d

@pytest.mark.skip('not asserting anything')
def test_total_latency(n=100):
    d = {}

    dfk = parsl.dfk()
    name = list(dfk.executors.keys())[0]

    start = time.time()
    double(99999).result()
    delta = time.time() - start
    print("[{}] Priming done in {:=10.3f}ms".format(name, delta * 1000))

    launch_latency = []
    for i in range(0, n):
        start = time.time()
        d[i] = double(i)
        d[i].result()
        delta = time.time() - start
        launch_latency.append(delta * 1000)

    print("[{}] Latency min:{:=10.3f}ms max:{:=10.3f}ms avg:{:=10.3f}ms".format(name,
                                                                                min(launch_latency),
                                                                                max(launch_latency),
                                                                                sum(launch_latency) / len(launch_latency)))

    with open("latency.{}.pkl".format(name), 'wb') as f:
        pickle.dump(launch_latency, f)
    print("[TEST STATMS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sitespec", default=None)
    parser.add_argument("-c", "--count", default="100",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.sitespec:
        config = None
        try:
            exec("import parsl; from {} import config".format(args.sitespec))
            parsl.load(config)
        except Exception:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)

    if args.debug:
        parsl.set_stream_logger()

    x = test_total_latency(int(args.count))
