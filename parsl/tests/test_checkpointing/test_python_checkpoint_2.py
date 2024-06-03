import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.tests.configs.local_threads_checkpoint import fresh_config


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
    return uuid.uuid4()


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Load memoization table from previous checkpoint
    """
    with parsl_configured(tmpd_cwd, checkpoint_mode="task_exit"):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        result = uuid_app().result()

    with parsl_configured(tmpd_cwd, checkpoint_files=checkpoint_files):
        relaunched = uuid_app().result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"
