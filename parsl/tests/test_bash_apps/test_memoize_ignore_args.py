import os

import pytest

import parsl
from parsl.app.app import bash_app


@bash_app(cache=True, ignore_for_cache=['x'])
def oneshot_app(x):
    if x != 0:
        raise RuntimeError("App was executed when it should not have been")
    return "true"


def test_memo_same_at_definition():
    oneshot_app(x=0).result()  # this should be executed
    oneshot_app(x=1).result()  # this should be memoized, and will raise a RuntimeError if actually executed


@bash_app(cache=True, ignore_for_cache=['stdout'])
def no_checkpoint_stdout_app_ignore_args(stdout=None):
    return "echo X"


@pytest.mark.shared_fs
def test_memo_stdout(tmpd_cwd):
    path_x = tmpd_cwd / "test.memo.stdout.x"

    # this should run and create a file named after path_x
    no_checkpoint_stdout_app_ignore_args(stdout=str(path_x)).result()
    assert path_x.exists()

    # this should be memoized, so should not get created
    path_y = tmpd_cwd / "test.memo.stdout.y"

    no_checkpoint_stdout_app_ignore_args(stdout=path_y).result()
    assert not path_y.exists(), "For memoization, expected NO file written"

    # this should also be memoized, so not create an arbitrary name
    z_fut = no_checkpoint_stdout_app_ignore_args(stdout=parsl.AUTO_LOGNAME)
    z_fut.result()
    assert not os.path.exists(z_fut.stdout)
