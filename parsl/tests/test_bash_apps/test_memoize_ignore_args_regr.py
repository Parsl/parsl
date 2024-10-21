import copy
from typing import List

import pytest

import parsl
from parsl.app.app import bash_app

const_list_x: List[str] = []
const_list_x_arg: List[str] = copy.deepcopy(const_list_x)

assert const_list_x == const_list_x_arg


@bash_app(cache=True, ignore_for_cache=['x'])
def oneshot_app(x):
    if x != 0:
        raise RuntimeError("App was executed when it should not have been")
    return "true"


def test_memo_same_at_definition():
    oneshot_app(x=0).result()  # this should be executed
    oneshot_app(x=1).result()  # this should be memoized, and will raise a RuntimeError if actually executed


@bash_app(cache=True, ignore_for_cache=const_list_x_arg)
def no_checkpoint_stdout_app(stdout=None):
    return "echo X"


@pytest.mark.shared_fs
def test_memo_stdout(tmpd_cwd):
    assert const_list_x == const_list_x_arg

    path_x = tmpd_cwd / "test.memo.stdout.x"

    # this should run and create a file named after path_x
    no_checkpoint_stdout_app(stdout=str(path_x)).result()
    path_x.unlink(missing_ok=False)

    no_checkpoint_stdout_app(stdout=str(path_x)).result()
    assert not path_x.exists(), "For memoization, expected NO file written"

    # this should also be memoized, so not create an arbitrary name
    z_fut = no_checkpoint_stdout_app(stdout=parsl.AUTO_LOGNAME)
    z_fut.result()
    assert const_list_x == const_list_x_arg
