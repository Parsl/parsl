import argparse

import pytest

import parsl
from parsl.app.app import python_app  # , bash_app
from parsl.tests.site_tests.site_config_selector import fresh_config


@python_app
def platform(sleep=10, stdout=None):
    import platform
    import time
    time.sleep(sleep)
    return platform.uname()


@pytest.mark.local
@pytest.mark.skip("The behaviour this test is testing is unclear: there is no guarantee that tasks will go to different nodes")
def test_platform(n=2, sleep_dur=10):
    """ This should sleep to make sure that concurrent apps will go to different workers
    on different nodes.
    """
    config = fresh_config()
    if config.executors[0].label == "htex_local":
        return

    parsl.load(fresh_config())

    dfk = parsl.dfk()
    name = list(dfk.executors.keys())[0]
    print("Trying to get executor : ", name)

    x = [platform(sleep=1) for i in range(2)]
    print([i.result() for i in x])

    print("Executor : ", dfk.executors[name])
    print("Connected   : ", dfk.executors[name].connected_workers)
    print("Outstanding : ", dfk.executors[name].outstanding)

    d = []
    for i in range(0, n):
        x = platform(sleep=sleep_dur)
        d.append(x)

    pinfo = set([i.result()for i in d])
    assert len(pinfo) == 2, "Expected two nodes, instead got {}".format(pinfo)

    print("Test passed")

    dfk.cleanup()
    parsl.clear()
    return True


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="4",
                        help="Count of apps to launch")
    parser.add_argument("-t", "--time", default="60",
                        help="Sleep time for each app")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_platform(n=int(args.count), sleep_dur=int(args.time))
