import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor


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
def uuid_app():
    import uuid
    return uuid.uuid4()


@python_app(cache=True)
def uuid_in_exception_app():
    import uuid
    raise RuntimeError(uuid.uuid4())


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Load memoization table from previous checkpoint
    """
    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit")):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        result = uuid_app().result()

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_files=checkpoint_files)):
        relaunched = uuid_app().result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"


@pytest.mark.local
def test_regression_239(tmpd_cwd):
    """Check that a failing app is run again in a subsequent run.

    This doesn't check that the app is not recorded in the checkpoint database,
    only that its exception is not re-used at the task execution level.
    """
    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit")):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        result = uuid_in_exception_app().exception()

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_files=checkpoint_files)):
        relaunched = uuid_in_exception_app().exception()

    assert result.args[0] != relaunched.args[0], "RuntimeError UUIDs should be different"
