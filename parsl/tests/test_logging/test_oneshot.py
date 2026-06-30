import pickle
import unittest
import uuid
from unittest.mock import Mock

import pytest

import parsl.logconfigs.base as b


@pytest.mark.local
def test_simple(tmpd_cwd):
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    lc.initialize_logging.assert_not_called()
    b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_simple")
    lc.initialize_logging.assert_called()

    cb = b._initialized_log_contexts[lc.uuid].uninit_callback

    cb.assert_not_called()
    b.oneshot_uninitialize_logging(log_config=lc)
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


@pytest.mark.local
def test_nested(tmpd_cwd):
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_1")
    lc.initialize_logging.assert_called_once()

    b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_2")
    lc.initialize_logging.assert_called_once()

    cb = b._initialized_log_contexts[lc.uuid].uninit_callback

    cb.assert_not_called()
    b.oneshot_uninitialize_logging(log_config=lc)
    cb.assert_not_called()
    b.oneshot_uninitialize_logging(log_config=lc)
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


@pytest.mark.local
def test_sequential(tmpd_cwd):
    """Test that if a log context can be released completely, and then reinitialized."""
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_1")
    lc.initialize_logging.assert_called_once()
    cb = b._initialized_log_contexts[lc.uuid].uninit_callback
    cb.assert_not_called()
    b.oneshot_uninitialize_logging(log_config=lc)
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition, between uses"

    b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_2")
    assert lc.initialize_logging.call_count == 2, "Should have been initialized once per block"

    cb2 = b._initialized_log_contexts[lc.uuid].uninit_callback

    # There is no requirement that cb2 be a fresh callback (mock) or be re-used.
    # When writing this test, cb2 is a reused mock, `cb2 is cb`, but that isn't
    # a requirement of the API.
    c = cb2.call_count
    b.oneshot_uninitialize_logging(log_config=lc)
    assert cb2.call_count == c + 1

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


class NestedPickleTestConfig(b.LogConfig):
    def initialize_logging(self, *args, **kwargs):
        def de():
            pass
        return de


@pytest.mark.local
def test_nested_pickle(tmpd_cwd):

    # this config will never be exposed to the log system except after passing
    # through pickle via two different paths, to generate different "remote-like"
    # objects.
    lc_orig = NestedPickleTestConfig()

    p = pickle.dumps(lc_orig)

    assert lc_orig.uuid not in b._initialized_log_contexts
    lc_1 = pickle.loads(p)
    b.oneshot_initialize_logging(log_config=lc_1, log_dir=tmpd_cwd, log_name="test_nested_1")

    assert lc_orig.uuid in b._initialized_log_contexts
    lc_2 = pickle.loads(p)
    b.oneshot_initialize_logging(log_config=lc_2, log_dir=tmpd_cwd, log_name="test_nested_2")

    assert lc_orig.uuid in b._initialized_log_contexts

    b.oneshot_uninitialize_logging(log_config=lc_2)
    assert lc_orig.uuid in b._initialized_log_contexts

    b.oneshot_uninitialize_logging(log_config=lc_1)
    assert lc_orig.uuid not in b._initialized_log_contexts


@pytest.mark.local
def test_overlap_pickle(tmpd_cwd):

    # this config will never be exposed to the log system except after passing
    # through pickle via two different paths, to generate different "remote-like"
    # objects.
    lc_orig = NestedPickleTestConfig()

    p = pickle.dumps(lc_orig)

    assert lc_orig.uuid not in b._initialized_log_contexts
    lc_1 = pickle.loads(p)
    b.oneshot_initialize_logging(log_config=lc_1, log_dir=tmpd_cwd, log_name="test_nested_1")

    assert lc_orig.uuid in b._initialized_log_contexts
    lc_2 = pickle.loads(p)
    b.oneshot_initialize_logging(log_config=lc_2, log_dir=tmpd_cwd, log_name="test_nested_2")

    assert lc_orig.uuid in b._initialized_log_contexts

    b.oneshot_uninitialize_logging(log_config=lc_2)
    assert lc_orig.uuid in b._initialized_log_contexts

    b.oneshot_uninitialize_logging(log_config=lc_1)
    assert lc_orig.uuid not in b._initialized_log_contexts
