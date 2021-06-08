import argparse

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import fresh_config


local_config = fresh_config()
local_config.retries = 2


@python_app
def sleep_fail(sleep_dur, sleep_rand_max, fail_prob, inputs=[]):
    import time
    import random

    s = sleep_dur + random.randint(-sleep_rand_max, sleep_rand_max)

    time.sleep(s)
    x = float(random.randint(0, 100)) / 100
    if x <= fail_prob:
        # print("Fail")
        raise Exception("App failure")
    else:
        pass
        # print("Succeed")


@python_app
def double(x):
    return x * 2


@pytest.mark.local
def test_simple(n=10):
    import time
    start = time.time()
    x = double(n)
    print("Result : ", x.result())
    assert x.result() == n * 2, (f"Expected double to return:{n * 2} "
                                 f"instead got:{x.result()}")
    print(f"Duration : {time.time() - start}s")
    print("[TEST STATUS] test_parallel_for [SUCCESS]")
    return True


@pytest.mark.skip('broken')
def test_no_deps(numtasks=10):
    """Test basic error handling, with no dependent failures
    """

    fus = []
    for i in range(0, 10):

        fu = sleep_fail(0.1, 0, .8)
        fus.extend([fu])

    count = 0
    for fu in fus:
        try:
            fu.result()
        except Exception as e:
            print("Caught exception : ", "*" * 20)
            print(e)
            print("*" * 20)
            count += 1

    print(f"Caught failures of  {count}/{len(fus)}")


@pytest.mark.skip('broken')
def test_fail_sequence(numtasks=10):
    """Test failure in a sequence of dependencies

    App1 -> App2 ... -> AppN
    """

    sleep_dur = 0.1
    fail_prob = 0.4

    fus = {0: None}
    for i in range(numtasks):
        print(f"Chaining {i + 1} to {fus[i]}")
        fus[i + 1] = sleep_fail(sleep_dur, 0, fail_prob, inputs=[fus[i]])

    # time.sleep(numtasks*sleep_dur)
    for k in sorted(fus.keys()):
        try:
            x = fus[i].result()
            print(f"{k} : {x}")
        except Exception as e:
            print(f"{k} : {e}")

    return


@pytest.mark.skip('broken')
def test_deps(numtasks=10):
    """Random failures in branches of Map -> Map -> reduce

    App1   App2  ... AppN
    """

    fus = []
    for i in range(0, numtasks):
        fu = sleep_fail(0.2, 0, .4)
        fus.extend([fu])

    # App1   App2  ... AppN
    # |       |        |
    # V       V        V
    # App1   App2  ... AppN

    fus_2 = []
    for fu in fus:
        fu = sleep_fail(0, 0, .8, inputs=[fu])
        fus_2.extend([fu])

    # App1   App2  ... AppN
    #   |       |        |
    #   V       V        V
    # App1   App2  ... AppN
    #    \      |       /
    #     \     |      /
    #       App_Final

    fu_final = sleep_fail(1, 0, 0, inputs=fus_2)

    try:
        print("Final status : ", fu_final.result())
    except parsl.dataflow.error.DependencyError as e:
        print("Caught the right exception")
        print("Exception : ", e)
    except Exception as e:
        assert 5 == 1, f"Expected DependencyError got: {e}"
    else:
        print("Shoot! no errors ")


@python_app
def sleep_then_fail(sleep_dur=0.1):
    import time
    import math
    time.sleep(sleep_dur)
    math.ceil("Trigger TypeError")
    return 0


@pytest.mark.skip('broken')
def test_fail_nowait(numtasks=10):
    """Test basic error handling, with no dependent failures
    """
    import time
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(sleep_dur=0.1)
        fus.extend([fu])

    try:
        [x.result() for x in fus]
    except Exception as e:
        assert isinstance(e, TypeError), f"Expected a TypeError, got {e}"

    # fus[0].result()
    time.sleep(1)
    print("Done")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_simple()
    # test_fail_nowait(numtasks=int(args.count))
    # test_no_deps(numtasks=int(args.count))
    # test_fail_sequence(numtasks=int(args.count))
    # test_deps(numtasks=int(args.count))
