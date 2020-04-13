import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def sleep_fail(sleep_dur, sleep_rand_max, fail_prob, inputs=[]):
    import time
    import random

    s = sleep_dur + random.randint(-sleep_rand_max, sleep_rand_max)

    time.sleep(s)
    raise Exception("App failure")


def test_no_deps(numtasks=2):
    """Test basic error handling, with no dependent failures
    """

    fus = []
    for i in range(0, numtasks):

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

    print("Caught failures of  {0}/{1}".format(count, len(fus)))


def test_fail_sequence(numtasks=2):
    """Test failure in a sequence of dependencies

    App1 -> App2 ... -> AppN
    """

    sleep_dur = 0.1
    fail_prob = 0.4

    fus = {0: None}
    for i in range(0, numtasks):
        print("Chaining {0} to {1}".format(i + 1, fus[i]))
        fus[i + 1] = sleep_fail(sleep_dur, 0, fail_prob, inputs=[fus[i]])

    # time.sleep(numtasks*sleep_dur)
    for k in sorted(fus.keys()):
        try:
            x = fus[i].result()
            print("{0} : {1}".format(k, x))
        except Exception as e:
            print("{0} : {1}".format(k, e))

    return


def test_deps(numtasks=2):
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
    # App_Final

    fu_final = sleep_fail(1, 0, 0, inputs=fus_2)

    try:
        print("Final status : ", fu_final.result())
    except parsl.dataflow.error.DependencyError as e:
        print("Caught the right exception")
        print("Exception : ", e)
    except Exception as e:
        assert False, "Expected DependencyError but got: %s" % e
    else:
        raise RuntimeError("Expected DependencyError, but got no exception")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_no_deps(numtasks=int(args.count))
    test_fail_sequence(numtasks=int(args.count))
