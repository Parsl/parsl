import logging

import pytest

# Typeguard does some odd trickery that is apparently confusing to mypy
from typeguard import TypeCheckError  # type: ignore[attr-defined]

from parsl.app.app import bash_app
from parsl.app.errors import BadStdStreamFile


@bash_app
def echo_to_streams(msg, stderr=None, stdout=None):
    return 'echo "{0}"; echo "{0}" >&2'.format(msg)


speclist = (
    (TypeCheckError, ['t3.out', 'w']),
    (TypeCheckError, ('t4.out', None)),
    (TypeCheckError, (42, 'w')),
    (TypeCheckError, ('t6.out', 'w', 42)),
    (TypeCheckError, ('t7.out',)),
    (BadStdStreamFile, ('t8.out', 'badmode')),
)

testids = (
    'list_not_tuple',
    'null_mode',
    'not_a_string',
    '3tuple',
    '1tuple',
    'bad_mode',
)


@pytest.mark.parametrize('err_type,spec', speclist, ids=testids)
def test_bad_stdout_specs(err_type, spec):
    """Testing bad stdout spec cases"""
    with pytest.raises(err_type):
        echo_to_streams("Hello world", stdout=spec, stderr='t.err').result()


@pytest.mark.issue3328
@pytest.mark.unix_filesystem_permissions_required
def test_bad_stdout_file():
    """Testing bad stderr file"""
    o = "/bad/dir/t2.out"
    with pytest.raises(BadStdStreamFile):
        echo_to_streams("Hello world", stdout=o, stderr='t.err').result()


@pytest.mark.issue3328
@pytest.mark.unix_filesystem_permissions_required
def test_bad_stderr_file():
    """Testing bad stderr file"""

    err = "/bad/dir/t2.err"
    with pytest.raises(BadStdStreamFile):
        echo_to_streams("Hello world", stderr=err).result()


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
