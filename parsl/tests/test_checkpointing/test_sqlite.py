import contextlib
import os

import pytest

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.dataflow.memoizers.sqlite import SQLiteMemoizer


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
    import uuid
    raise RuntimeError(str(uuid.uuid4()))


@pytest.mark.local
def test_no_exceptions(tmpd_cwd):
    with parsl_configured(tmpd_cwd, SQLiteMemoizer(checkpoint_dir=tmpd_cwd)):
        e = exception_app().exception()
        assert e is not None, "exception_app should have produced an exception"
        uuid1 = e.args[0]

    with parsl_configured(tmpd_cwd, SQLiteMemoizer(checkpoint_dir=tmpd_cwd)):
        e = exception_app().exception()
        assert e is not None, "exception_app should have produced an exception"
        uuid2 = e.args[0]

    assert uuid1 != uuid2, f"exception_app invocations should have produced fresh exceptions: {uuid1} vs {uuid2}"
