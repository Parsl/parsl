import argparse
import os

import pytest

import parsl
import parsl.app.errors as perror
from parsl.app.app import App
from parsl.tests.configs.local_threads import config

parsl.clear()
dfk = parsl.load(config)


@App('bash')
def echo_to_streams(msg, stderr='std.err', stdout='std.out'):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')


@pytest.mark.whitelist(whitelist, reason='broken in IPP')
def test_bad_stdout():
    """Testing bad stdout file
    """
    stdout = "/x/test_bad_stdout.stdout"
    stderr = "test_bad_stdout.stderr"
    fu = echo_to_streams("Hello world", stderr=stderr, stdout=stdout)

    try:
        fu.result()
    except Exception as e:
        assert isinstance(
            e, perror.BadStdStreamFile), "Expected BadStdStreamFile, got :{0}".format(type(e))

    return


@pytest.mark.whitelist(whitelist, reason='broken in IPP')
def test_bad_stderr():
    """Testing bad stderr file
    """
    stdout = "test_bad_stdout.stdout"
    stderr = "/x/test_bad_stdout.stderr"
    fu = echo_to_streams("Hello world", stderr=stderr, stdout=stdout)

    try:
        fu.result()
    except Exception as e:
        assert isinstance(
            e, perror.BadStdStreamFile), "Expected BadStdStreamFile, got :{0}".format(type(e))

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    y = test_bad_stdout()
    y = test_bad_stderr()
