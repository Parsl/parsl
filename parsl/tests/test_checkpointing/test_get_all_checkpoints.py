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
def uuid_app(x):
    import uuid
    return uuid.uuid4()


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Tests default loading of all checkpoint files from previous runs.
    """
    import uuid
    param = str(uuid.uuid4())

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit")):
        result = uuid_app(param).result()

    with parsl_configured(tmpd_cwd, BasicMemoizer(checkpoint_mode="task_exit")):
        relaunched = uuid_app(param).result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"
