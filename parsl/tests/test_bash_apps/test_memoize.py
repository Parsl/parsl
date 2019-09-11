import argparse
import os

import pytest

import parsl
from parsl.app.app import App
from parsl.tests.configs.local_threads import config
from parsl.app.errors import AppFailure


@App('bash', cache=True)
def fail_on_presence(outputs=[]):
    return 'if [ -f {0} ] ; then exit 1 ; else touch {0}; fi'.format(outputs[0])


@pytest.mark.xfail(reason="This failing test demonstrates broken @bash_app checkpointing", strict=True)
def test_bash_memoization(n=2):
    """Testing bash memoization
    """
    temp_filename = "test.memoization.tmp"

    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    print("Launching: ", n)
    x = fail_on_presence(outputs=[temp_filename])
    x.result()

    d = {}
    for i in range(0, n):
        d[i] = fail_on_presence(outputs=[temp_filename])

    print("Waiting for results from round1")
    for i in d:
        assert isinstance(d[i].exception(), AppFailure)


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

    x = test_bash_memoization(n=4)
