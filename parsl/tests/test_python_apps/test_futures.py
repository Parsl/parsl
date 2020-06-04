"""Testing behavior of futures

We have the following cases for AppFutures:

1. App launched immmediately on call
2. App launch was delayed (due to unresolved dep(s))

Same applies to datafutures, and we need to know the behavior wrt.

1. result() called on 1, vs 2
2. done() called on 1, vs 2

"""
import argparse
import os
import pytest

import parsl
from parsl.app.app import python_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads import config


@python_app
def delay_incr(x, delay=0, outputs=[]):
    import time
    if outputs:
        with open(outputs[0].filepath, 'w') as outs:
            outs.write(str(x + 1))
    time.sleep(delay)
    return x + 1


def get_contents(filename):
    c = None
    with open(filename, 'r') as f:
        c = f.read()
    return c


def test_fut_case_1():
    """Testing the behavior of AppFutures where there are no dependencies
    """

    app_fu = delay_incr(1, delay=0.5)

    status = app_fu.done()
    result = app_fu.result()

    print("Status : ", status)
    print("Result : ", result)

    assert result == 2, 'Output does not match expected 2, goot: "{0}"'.format(
        result)
    return True


@pytest.mark.staging_required
def test_fut_case_2():
    """Testing the behavior of DataFutures where there are no dependencies
    """
    output_f = 'test_fut_case_2.txt'
    app_fu = delay_incr(1, delay=10, outputs=[File(output_f)])
    data_fu = app_fu.outputs[0]

    data_fu.done()
    result = data_fu.result().filepath
    print("App_fu  : ", app_fu)
    print("Data_fu : ", data_fu)

    assert os.path.basename(result) == output_f, \
        "DataFuture did not return the filename, got : {0}".format(result)
    print("Status : ", data_fu.done())
    print("Result : ", result)

    contents = get_contents(result)
    assert contents == '2', 'Output does not match expected "2", got: "{0}"'.format(
        contents)
    return True


def test_fut_case_3():
    """Testing the behavior of AppFutures where there are dependencies

    The first call has a delay of 0.5s, and the second call depends on the first
    """

    app_1 = delay_incr(1, delay=0.5)
    app_2 = delay_incr(app_1)

    status = app_2.done()
    result = app_2.result()

    print("Status : ", status)
    print("Result : ", result)

    assert result == 3, 'Output does not match expected 2, goot: "{0}"'.format(
        result)
    return True


@pytest.mark.staging_required
def test_fut_case_4():
    """Testing the behavior of DataFutures where there are dependencies

    The first call has a delay of 0.5s, and the second call depends on the first
    """
    """Testing the behavior of DataFutures where there are no dependencies
    """
    output_f1 = 'test_fut_case_4_f1.txt'
    output_f2 = 'test_fut_case_4_f2.txt'
    app_1 = delay_incr(1, delay=0.5, outputs=[File(output_f1)])
    app_1.outputs[0]
    app_2 = delay_incr(app_1, delay=0.5, outputs=[File(output_f2)])
    data_2 = app_2.outputs[0]

    status = data_2.done()
    result = data_2.result().filepath
    print("App_fu  : ", app_2)
    print("Data_fu : ", data_2)

    print("Status : ", status)
    print("Result : ", result)

    assert os.path.basename(result) == output_f2, \
        "DataFuture did not return the filename, got : {0}".format(result)

    contents = get_contents(result)
    assert contents == '3', 'Output does not match expected "3", got: "{0}"'.format(
        result)
    return True


if __name__ == '__main__':
    parsl.clear()
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # x = test_parallel_for(int(args.count))
    # y = test_fut_case_2()
    # y = test_fut_case_3()
    y = test_fut_case_4()
    # raise_error(0)
