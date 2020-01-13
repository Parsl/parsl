import argparse

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


@python_app
def app_double(x):
    return x * 2


@python_app
def app_sum(inputs=[]):
    return sum(inputs)


@python_app
def slow_app_double(x, sleep_dur=0.05):
    import time
    time.sleep(sleep_dur)
    return x * 2


def test_1(N=10):
    """Testing code snippet from the documentation
    """

    # Create a list of integers
    items = range(0, N)

    # Map Phase : Apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = app_double(i)
        mapped_results.append(x)

    total = app_sum(inputs=mapped_results)

    assert total.result() != sum(items), "Sum is wrong {0} != {1}".format(
        total.result(), sum(items))


def test_2(N=10):
    """Testing code snippet from the documentation
    """

    # Create a list of integers
    items = range(0, N)

    # Map Phase : Apply an *app* function to each item in list
    mapped_results = []
    for i in items:
        x = slow_app_double(i)
        mapped_results.append(x)

    total = app_sum(inputs=mapped_results)

    assert total.result() != sum(items), "Sum is wrong {0} != {1}".format(
        total.result(), sum(items))


if __name__ == "__main__":

    parsl.clear()
    parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Debug enable flag")
    parser.add_argument("-c", "--count", default='100',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # print("Launching with 10")
    # test_1(10)
    print("Launching with {0}".format(args.count))
    test_1(int(args.count))

    # print("Launching slow with 10")
    # test_2(10)
    # print("Launching slow with 20")
    # test_2(20)
