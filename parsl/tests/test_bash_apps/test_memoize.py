import pytest

from parsl import File
from parsl.app.app import bash_app


@bash_app(cache=True)
def fail_on_presence(outputs=()):
    return 'if [ -f {0} ] ; then exit 1 ; else touch {0}; fi'.format(outputs[0])


# This test is an oddity that requires a shared-FS and simply
# won't work if there's a staging provider.
# @pytest.mark.sharedFS_required
@pytest.mark.issue363
def test_bash_memoization(tmpd_cwd, n=2):
    """Testing bash memoization
    """
    mpath = tmpd_cwd / "test.memoization.tmp"
    temp_file = File(str(mpath))
    fail_on_presence(outputs=[temp_file]).result()

    futs = [fail_on_presence(outputs=[temp_file]) for _ in range(n)]
    for f in futs:
        assert f.exception() is None


@bash_app(cache=True)
def fail_on_presence_kw(outputs=(), foo=None):
    return 'if [ -f {0} ] ; then exit 1 ; else touch {0}; fi'.format(outputs[0])


# This test is an oddity that requires a shared-FS and simply
# won't work if there's a staging provider.
# @pytest.mark.sharedFS_required
@pytest.mark.issue363
def test_bash_memoization_keywords(tmpd_cwd, n=2):
    """Testing bash memoization
    """
    mpath = tmpd_cwd / "test.memoization.tmp"
    temp_file = File(str(mpath))

    foo = {"a": 1, "b": 2}
    fail_on_presence_kw(outputs=[temp_file], foo=foo).result()

    futs = [fail_on_presence_kw(outputs=[temp_file], foo=foo) for _ in range(n)]
    for f in futs:
        assert f.exception() is None
