"""Testing bash apps
"""
import parsl
from parsl import *

import argparse

# parsl.set_stream_logger()

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def random():
    import random
    return random.randint(1, 10)


@App('python', dfk)
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x + 1


@App('python', dfk)
def join(inputs=[]):
    return sum(inputs)


def test_xor_split():
    """Test XOR split. Do A if x else B
    """
    x = random()

    if x.result() > 5:
        print("Result > 5")
    else:
        print("Result < 5")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="10",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # test_increment(depth=int(args.width))
    test_xor_split()
