"""
What is this test
=================

This simple test measures the time taken to launch single tasks and receive results from an executor.

We will call get the result back after launch as ``latency`` and the combined measure of time to launch
and the time to get the result back as ``roundtrip time``.


How do we measure
=================

1. We start the executor
2. Run 100 tasks and wait for them to prime the executors and the ensure that workers are online
3. Run the actual measurements:
     1) Time to launch the app
     2) Time to receive result.

4. Take the min, max, and mean of ``latency`` and ``roundtrip``.


Preliminary results
==================

Results from running on Midway with IPP executor.

Latency   |   Min:0.005968570709228516 Max:0.011006593704223633 Average:0.0065019774436950685
Roundtrip |   Min:0.00716400146484375  Max:0.012288331985473633 Average:0.007741005420684815
"""
import time

import parsl


@parsl.python_app
def python_app():
    import platform
    return f"Hello from {platform.uname()}"


@parsl.python_app
def python_app_slow(duration):
    import platform
    import time
    time.sleep(duration)
    return f"Hello from {platform.uname()}"


@parsl.python_app
def python_noop():
    return


@parsl.bash_app
def bash_app(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


def test_python_remote(count=2):
    """ Run with no delay.
    """
    fus = []
    for i in range(0, count):
        fu = python_app_slow(0)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


def test_python_remote_slow(count=2):
    fus = []
    for i in range(0, count):
        fu = python_app_slow(count)
        fus.extend([fu])

    for fu in fus:
        print(fu.result())


def average(x):
    return sum(x) / len(x)


def test_python(count):
    results = {}

    print("Priming the system")
    items = []
    for i in range(0, 100):
        items.extend([python_app()])
    print("Launched primer application")
    for i in items:
        i.result()
    print("Primer done")

    latency = []
    rtt = []
    for i in range(0, 100):
        pre = time.time()
        results[i] = python_noop()
        post = time.time()
        results[i].result()
        final = time.time()
        latency.append(final - post)
        rtt.append(final - pre)

    min_latency = min(latency) * 1000
    max_latency = max(latency) * 1000
    avg_latency = average(latency) * 1000
    print(f"Latency   |   Min:{min_latency:0.3}ms Max:{max_latency:0.3}ms "
          f"Average:{avg_latency:0.3}ms")

    min_rtt = min(rtt) * 1000
    max_rtt = max(rtt) * 1000
    avg_rtt = average(rtt) * 1000

    print(f"Roundtrip |   Min:{min_rtt:0.3}ms Max:{max_rtt:0.3}ms "
          f"Average:{avg_rtt:0.3}ms")


def test_bash():
    import os
    fname = os.path.basename(__file__)

    x = bash_app(stdout=f"{fname}.out")
    print("Waiting ....")
    print(x.result())
