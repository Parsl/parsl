import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoization import BasicMemoizer
from parsl.executors.threads import ThreadPoolExecutor


class CheckpointExceptionsMemoizer(BasicMemoizer):
    def filter_for_checkpoint(self, app_fu):
        # checkpoint everything, rather than selecting only futures with
        # results, not exceptions.

        # task record is available from app_fu.task_record
        assert app_fu.task_record is not None

        return True


def fresh_config():
    return Config(
        memoizer=CheckpointExceptionsMemoizer(),
        executors=[
            ThreadPoolExecutor(
                label='local_threads_checkpoint',
            )
        ]
    )


@contextlib.contextmanager
def parsl_configured(run_dir, **kw):
    c = fresh_config()
    c.run_dir = run_dir
    for config_attr, config_val in kw.items():
        setattr(c, config_attr, config_val)
    dfk = parsl.load(c)
    for ex in dfk.executors.values():
        ex.working_dir = run_dir
    yield dfk

    parsl.dfk().cleanup()


@python_app(cache=True)
def uuid_app():
    import uuid
    raise RuntimeError(str(uuid.uuid4()))


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Load memoization table from previous checkpoint
    """
    with parsl_configured(tmpd_cwd, checkpoint_mode="task_exit"):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        result = uuid_app().exception()

    with parsl_configured(tmpd_cwd, checkpoint_files=checkpoint_files):
        relaunched = uuid_app().exception()

    assert result.args == relaunched.args, "Expected following call to uuid_app to return cached uuid in exception"
