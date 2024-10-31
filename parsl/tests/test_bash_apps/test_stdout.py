import logging
import os

import pytest
import typeguard

import parsl.app.errors as perror
from parsl.app.app import bash_app


@bash_app
def echo_to_streams(msg, stderr=None, stdout=None):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)


whitelist = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', '*threads*')

speclist = (
    ['t3.out', 'w'],
    ('t4.out', None),
    (42, 'w'),
    ('t6.out', 'w', 42),
    ('t7.out',),
    ('t8.out', 'badmode')
)

testids = [
    'list_not_tuple',
    'null_mode',
    'not_a_string',
    '3tuple',
    '1tuple',
    'bad_mode'
]


@pytest.mark.parametrize('spec', speclist, ids=testids)
def test_bad_stdout_specs(spec):
    """Testing bad stdout spec cases"""

    fn = echo_to_streams("Hello world", stdout=spec, stderr='t.err')

    try:
        fn.result()
    except Exception as e:
        # This tests for TypeCheckError by string matching on the type name
        # because that class does not exist in typeguard 2.x - it is new in
        # typeguard 4.x. When typeguard 2.x support is dropped, this test can
        # become an isinstance check.
        assert "TypeCheckError" in str(type(e)) or isinstance(e, TypeError) or isinstance(e, perror.BadStdStreamFile), "Exception is wrong type"
    else:
        assert False, "Did not raise expected exception"


@pytest.mark.issue3328
@pytest.mark.unix_filesystem_permissions_required
def test_bad_stdout_file():
    """Testing bad stderr file"""

    o = "/bad/dir/t2.out"

    fn = echo_to_streams("Hello world", stdout=o, stderr='t.err')

    try:
        fn.result()
    except perror.BadStdStreamFile:
        pass
    else:
        assert False, "Did not raise expected exception BadStdStreamFile"

    return


@pytest.mark.issue3328
@pytest.mark.unix_filesystem_permissions_required
def test_bad_stderr_file():
    """Testing bad stderr file"""

    err = "/bad/dir/t2.err"

    fn = echo_to_streams("Hello world", stderr=err)

    try:
        fn.result()
    except perror.BadStdStreamFile:
        pass
    else:
        assert False, "Did not raise expected exception BadStdStreamFile"

    return


@pytest.mark.executor_supports_std_stream_tuples
@pytest.mark.shared_fs
def test_stdout_truncate(tmpd_cwd, caplog):
    """Testing truncation of prior content of stdout"""

    out = (str(tmpd_cwd / 't1.out'), 'w')
    err = str(tmpd_cwd / 't1.err')

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out[0]).readlines())

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out[0]).readlines())

    assert len1 == 1
    assert len1 == len2

    for record in caplog.records:
        assert record.levelno < logging.ERROR


@pytest.mark.shared_fs
def test_stdout_append(tmpd_cwd, caplog):
    """Testing appending to prior content of stdout (default open() mode)"""

    out = str(tmpd_cwd / 't1.out')
    err = str(tmpd_cwd / 't1.err')

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len1 = len(open(out).readlines())

    echo_to_streams('hi', stdout=out, stderr=err).result()
    len2 = len(open(out).readlines())

    assert len1 == 1 and len2 == 2

    for record in caplog.records:
        assert record.levelno < logging.ERROR
