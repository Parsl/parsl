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
def no_checkpoint_stdout_app(stdout=None):
    return "echo X"


@pytest.mark.issue363
def test_memo_stdout():

    # this should run and create a file named after path_x
    path_x = "test.memo.stdout.x"
    if os.path.exists(path_x):
        os.remove(path_x)

    no_checkpoint_stdout_app(stdout=path_x).result()
    assert os.path.exists(path_x)

    # this should be memoized, so not create benc.test.y
    path_y = "test.memo.stdout.y"
    assert not os.path.exists(path_y)
    no_checkpoint_stdout_app(stdout=path_y).result()
    assert not os.path.exists(path_y)

    # this should also be memoized, so not create an arbitrary name
    z_fut = no_checkpoint_stdout_app(stdout=parsl.AUTO_LOGNAME)
    z_fut.result()
    assert not os.path.exists(z_fut.stdout)
