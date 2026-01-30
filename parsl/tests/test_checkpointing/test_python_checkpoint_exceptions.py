import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor


class CheckpointExceptionsMemoizer(BasicMemoizer):
    def filter_exception_for_checkpoint(self, ex):
        # TODO: this used to be the case, but in moving to results-only mode,
        # the task record is lost. Maybe it's useful to pass it in? What
        # are the use cases for this deciding function?
        # task record is available from app_fu.task_record
        # assert app_fu.task_record is not None

        # override the default always-False, to be always-True
        return True


def fresh_config(run_dir, memoizer):
    return Config(
        memoizer=memoizer,
        run_dir=str(run_dir)
    )


@python_app(cache=True)
def uuid_app():
    import uuid
    raise RuntimeError(str(uuid.uuid4()))


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Load memoization table from previous checkpoint
    """
    with parsl.load(fresh_config(tmpd_cwd, CheckpointExceptionsMemoizer(checkpoint_mode="task_exit"))):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        result = uuid_app().exception()

    with parsl.load(fresh_config(tmpd_cwd, CheckpointExceptionsMemoizer(checkpoint_files=checkpoint_files))):
        relaunched = uuid_app().exception()

    assert result.args == relaunched.args, "Expected following call to uuid_app to return cached uuid in exception"
