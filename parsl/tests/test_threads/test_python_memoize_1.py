import parsl
from parsl import *
import time
import argparse

# parsl.set_stream_logger()
config = {
    "sites": [
        {"site": "Local_Threads",
         "auth": {"channel": None},
         "execution": {
             "executor": "threads",
             "provider": None,
             "maxThreads": 4,
         }
         }],
    "globals": {"lazyErrors": True,
    }
}

dfk = DataFlowKernel(config=config)


@App('python', dfk, cache=True)
def random_uuid(x, cache=True):
    import uuid
    return str(uuid.uuid4())


def test_python_memoization(n=4):
    """Testing python memoization disable
    """
    x = random_uuid(0)
    print(x.result())

    for i in range(0, n):
        foo = random_uuid(0)
        print(foo.result())
        assert foo.result() == x.result(), "Memoized results were not used"


@App('bash', dfk, cache=True)
def slow_echo_to_file(msg, outputs=[], stderr='std.err', stdout='std.out'):
    return 'sleep 1; echo {0} > {outputs[0]}'


def test_bash_memoization(n=4):
    """Testing bash memoization
    """

    print("Launching : ", n)
    x = slow_echo_to_file("hello world", outputs=['h1.out'])
    x.result()

    start = time.time()
    d = {}
    for i in range(0, n):
        d[i] = slow_echo_to_file("hello world", outputs=['h1.out'])

    print("Waiting for results from round1")
    [d[i].result() for i in d]
    end = time.time()
    delta = end - start
    assert delta < 0.1, "Memoized results were not used"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_python_memoization(n=4)
    x = test_bash_memoization(n=4)
