import argparse
import time

import parsl

# Tested. Confirmed. Local X Local X SingleNodeLauncher
# from parsl.tests.configs.local_ipp import config

# Tested. Confirmed. ssh X Slurm X SingleNodeLauncher
# from parsl.tests.configs.midway_ipp import config

# Tested. Confirmed. ssh X Slurm X SingelNodeLauncher
# from parsl.tests.configs.midway_ipp_multicore import config

# Tested. Confirmed. ssh X Slurm X SrunLauncher
# from parsl.tests.configs.midway_ipp_multinode import config

# OSG requires python3.5 for testing. This test is inconsitent,
# breaks often depending on where the test lands
# Tested. Confirmed. ssh X Condor X single_node
# from parsl.tests.configs.osg_ipp_multinode import config

# Tested. Confirmed. ssh X Torque X AprunLauncher
# from parsl.tests.configs.swan_ipp import config

# Tested. Confirmed. ssh X Torque X AprunLauncher
# from parsl.tests.configs.swan_ipp_multinode import config

# from parsl.tests.configs.cooley_ssh_il_single_node import config

# Tested. Confirmed. local X GridEngine X singleNode
# from parsl.tests.configs.cc_in2p3_local_single_node import config

# from parsl.tests.configs.comet_ipp_multinode import config

# from parsl.tests.configs.htex_local import config
parsl.set_stream_logger()

# from htex_midway import config
# from htex_swan import config


from parsl.app.app import python_app  # , bash_app


@python_app
def double(x):
    return x * 2


@python_app
def echo(x, string, stdout=None):
    print(string)
    return x * 5


@python_app
def import_echo(x, string, stdout=None):
    import time
    time.sleep(0)
    print(string)
    return x * 5


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


def test_simple(n=2):
    start = time.time()
    x = double(n)
    print("Result : ", x.result())
    assert x.result() == n * \
        2, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_imports(n=2):
    start = time.time()
    x = import_echo(n, "hello world")
    print("Result : ", x.result())
    assert x.result() == n * \
        5, "Expected double to return:{0} instead got:{1}".format(
            n * 2, x.result())
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


def test_platform(n=2):
    # sync
    x = platform(sleep=0)
    print(x.result())

    d = []
    for i in range(0, n):
        x = platform(sleep=5)
        d.append(x)

    print(set([i.result()for i in d]))

    return True


def test_parallel_for(n=2):
    d = {}
    start = time.time()
    for i in range(0, n):
        d[i] = double(i)
        # time.sleep(0.01)

    assert len(
        d.keys()) == n, "Only {0}/{1} keys in dict".format(len(d.keys()), n)

    [d[i].result() for i in d]
    print("Duration : {0}s".format(time.time() - start))
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return d


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sitespec", default=None)
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.sitespec:
        c = None
        try:
            exec("import parsl; from {} import config".format(args.sitespec))
            parsl.load(c)
        except Exception:
            print("Failed to load the requested config : ", args.sitespec)
            exit(0)

    if args.debug:
        parsl.set_stream_logger()

    x = test_simple(int(args.count))
    # x = test_imports()
    # x = test_parallel_for()
    # x = test_parallel_for(int(args.count))
    # x = test_stdout()
    x = test_platform()
