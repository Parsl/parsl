import argparse
import os

import pytest

import parsl
import parsl.app.errors as perror
from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import config


@bash_app
def echo_to_streams(msg, stderr='std.err', stdout='std.out'):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')

speclist = (
    '/bad/dir/t.out',
    ['t3.out', 'w'],
    ('t4.out', None),
    (42, 'w'),
    ('t6.out', 'w', 42),
    ('t7.out',),
    ('t8.out', 'badmode')
)

testids = [
    'nonexistent_dir',
    'list_not_tuple',
    'null_mode',
    'not_a_string',
    '3tuple',
    '1tuple',
    'bad_mode'
]


@pytest.mark.issue363
@pytest.mark.parametrize('spec', speclist, ids=testids)
def test_bad_stdout_specs(spec):
    """Testing bad stdout spec cases
    """

    fn = echo_to_streams("Hello world", stdout=spec, stderr='t.err')

    try:
        fn.result()
    except Exception as e:
        assert isinstance(
            e, perror.BadStdStreamFile), "Expected BadStdStreamFile, got: {0}".format(type(e))
    else:
        assert False, "Did not raise expected exception BadStdStreamFile"

    return


@pytest.mark.issue363
def test_bad_stderr_file():

    """ Testing bad stderr file """

    out = "t2.out"
    err = "/bad/dir/t2.err"

    fn = echo_to_streams("Hello world", stdout=out, stderr=err)

    try:
        fn.result()
    except Exception as e:
        assert isinstance(
            e, perror.BadStdStreamFile), "Expected BadStdStreamFile, got: {0}".format(type(e))
    else:
        assert False, "Did not raise expected exception BadStdStreamFile"

    return


@pytest.mark.issue363
def test_stdout_truncate():

    """ Testing truncation of prior content of stdout """

    out = ('t1.out', 'w')
    err = 't1.err'
    os.system('rm -f ' + out[0] + ' ' + err)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out[0]).readlines())

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out[0]).readlines())

    assert len1 == len2 == 1, "Line count of output files should both be 1, but: len1={} len2={}".format(len1, len2)

    os.system('rm -f ' + out[0] + ' ' + err)


@pytest.mark.issue363
def test_stdout_append():

    """ Testing appending to prior content of stdout (default open() mode) """

    out = 't1.out'
    err = 't1.err'
    os.system('rm -f ' + out + ' ' + err)

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out).readlines())

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out).readlines())

    assert len1 == 1 and len2 == 2, "Line count of output files should be 1 and 2, but:  len1={} len2={}".format(len1, len2)

    os.system('rm -f ' + out + ' ' + err)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enable debug output to console")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    parsl.load(config)

    # test_bad_stdout_specs is omitted because it is called in a
    # more complicated parameterised fashion by pytest.

    y = test_bad_stderr_file()
    y = test_stdout_truncate()
    y = test_stdout_append()
