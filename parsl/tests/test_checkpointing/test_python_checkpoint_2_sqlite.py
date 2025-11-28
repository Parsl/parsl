import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memosql import SQLiteMemoizer


def parsl_configured(run_dir, memoizer):
    return parsl.load(Config(
        run_dir=str(run_dir),
        memoizer=memoizer
    ))


@python_app(cache=True)
def uuid_app():
    import uuid
    return uuid.uuid4()


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd):
    """Load memoization table from previous checkpoint
    """
    with parsl_configured(tmpd_cwd, SQLiteMemoizer(checkpoint_dir=tmpd_cwd)):
        result = uuid_app().result()

    with parsl_configured(tmpd_cwd, SQLiteMemoizer(checkpoint_dir=tmpd_cwd)):
        relaunched = uuid_app().result()

    assert result == relaunched, "Expected following call to uuid_app to return cached uuid"


@python_app(cache=True)
def exception_app():
    raise RuntimeError("this is exception app")


@pytest.mark.local
def test_checkpointing_exception(tmpd_cwd):
    with pytest.raises(RuntimeError):
        with parsl_configured(tmpd_cwd, SQLiteMemoizer(checkpoint_dir=tmpd_cwd)):
            _ = exception_app().result()
