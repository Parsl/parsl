"""Testing bash apps
"""
import parsl
import parsl.app.errors as perror
import argparse
from parsl import *

print("Parsl version: ", parsl.__version__)

# parsl.set_stream_logger()
workers = ThreadPoolExecutor(max_workers=8)
dfk = DataFlowKernel(executors=[workers])


@App('bash', dfk)
def echo_to_streams(msg, stderr='std.err', stdout='std.out'):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)


def _test_bad_stdout():
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


def _test_bad_stderr():
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
