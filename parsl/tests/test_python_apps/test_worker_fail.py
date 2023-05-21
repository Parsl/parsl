import argparse
import time

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def import_echo(x, string, sleep=0):
    import time
    time.sleep(sleep)
    print(string)
    return x * 5


def test_parallel_for(n=2):
    d = {}
    start = time.time()
    for i in range(0, n):
        d[i] = import_echo(2, "hello", sleep=2)
        # time.sleep(0.01)

    assert len(
        d.keys()) == n, "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d
