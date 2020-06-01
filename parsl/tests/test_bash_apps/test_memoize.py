import argparse
import os
import pytest

import parsl
from parsl import File
from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import config


@bash_app(cache=True)
def fail_on_presence(outputs=[]):
    return 'if [ -f {0} ] ; then exit 1 ; else touch {0}; fi'.format(outputs[0])


# This test is an oddity that requires a shared-FS and simply
# won't work if there's a staging provider.
# @pytest.mark.sharedFS_required
@pytest.mark.issue363
def test_bash_memoization(n=2):
    """Testing bash memoization
    """
    temp_filename = "test.memoization.tmp"
    temp_file = File(temp_filename)

    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    temp_file = File(temp_filename)

    print("Launching: ", n)
    x = fail_on_presence(outputs=[temp_file])
    x.result()

    d = {}
    for i in range(0, n):
        d[i] = fail_on_presence(outputs=[temp_file])

    for i in d:
        assert d[i].exception() is None


@bash_app(cache=True)
def fail_on_presence_kw(outputs=[], foo={}):
    return 'if [ -f {0} ] ; then exit 1 ; else touch {0}; fi'.format(outputs[0])


# This test is an oddity that requires a shared-FS and simply
# won't work if there's a staging provider.
# @pytest.mark.sharedFS_required
@pytest.mark.issue363
def test_bash_memoization_keywords(n=2):
    """Testing bash memoization
    """
    temp_filename = "test.memoization.tmp"
    temp_file = File("test.memoization.tmp")

    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    temp_file = File(temp_filename)

    print("Launching: ", n)
    x = fail_on_presence_kw(outputs=[temp_file], foo={"a": 1, "b": 2})
    x.result()

    d = {}
    for i in range(0, n):
        d[i] = fail_on_presence_kw(outputs=[temp_file], foo={"b": 2, "a": 1})

    for i in d:
        assert d[i].exception() is None


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
