import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor
from parsl.utils import get_last_checkpoint


def parsl_configured(run_dir, memoizer):
    return parsl.load(Config(
        run_dir=str(run_dir),
        executors=[
            ThreadPoolExecutor(
                label='local_threads_checkpoint',
            )
        ],
        memoizer=memoizer
    ))


@python_app(cache=True)
def uuid_app(x):
    import uuid
    return uuid.uuid4()


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Tests that get_last_checkpoint finds checkpoints from prior run
    """
    import uuid
    param = str(uuid.uuid4())

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=[])):
        result = uuid_app(param).result()

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=get_last_checkpoint(rundir=str(tmpd_cwd)))):
        relaunched = uuid_app(param).result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"

    # check that we don't lose the result just because it came from two runs ago,
    # by running the same test again.
    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=get_last_checkpoint(rundir=str(tmpd_cwd)))):
        relaunched = uuid_app(param).result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"


@pytest.mark.local
def test_loading_checkpoint_other_files(tmpd_cwd):
    """Tests that get_last_checkpoint ignores other files.

       This is a regression test for a bug where the alphabetically last
       per-run directory was being selected, and then ignored if it was
       not a checkpoint directory, for example when the monitoring.db
       file is also present (and alphabetically after any numbered
       run directories).
    """
    import uuid
    param = str(uuid.uuid4())

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=[])):
        result = uuid_app(param).result()

    with open(tmpd_cwd / "X", "w"):
        # don't need to write anything, as we only need the directory entry
        # which is made by the open call.
        pass

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=get_last_checkpoint(rundir=str(tmpd_cwd)))):
        relaunched = uuid_app(param).result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"


@pytest.mark.local
def test_high_rundir(tmpd_cwd):
    """Tests that get_last_checkpoint respects rundirs of more than 3 digits.
    """
    import uuid
    param = str(uuid.uuid4())

    with open(tmpd_cwd / "998", "w"):
        pass

    # this will have rundir "999"
    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=[])):
        assert "999" in parsl.dfk().run_dir
        result_999 = uuid_app(param).result()

    # this will have rundir "1000", lexically before 999
    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=[])):
        assert "1000" in parsl.dfk().run_dir
        result_1000 = uuid_app(param).result()

    assert result_999 != result_1000, "Should have not used previous result"

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit", checkpoint_files=get_last_checkpoint(rundir=str(tmpd_cwd)))):
        relaunched = uuid_app(param).result()

    assert result_1000 == relaunched, "Expected restore to be from 1000 rundir"
