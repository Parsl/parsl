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
    uninit = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_simple")
    lc.initialize_logging.assert_called()

    cb = b._initialized_log_contexts[lc.uuid].uninit_callback

    cb.assert_not_called()
    uninit()
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


@pytest.mark.local
def test_nested(tmpd_cwd):
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    uninit = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_1")
    lc.initialize_logging.assert_called_once()

    uninit2 = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_2")
    lc.initialize_logging.assert_called_once()

    cb = b._initialized_log_contexts[lc.uuid].uninit_callback

    cb.assert_not_called()
    uninit2()
    cb.assert_not_called()
    uninit()
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


@pytest.mark.local
def test_overlap(tmpd_cwd):
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    uninit = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_overlap_1")
    cb = b._initialized_log_contexts[lc.uuid].uninit_callback
    lc.initialize_logging.assert_called_once()

    uninit2 = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_overlap_2")
    lc.initialize_logging.assert_called_once()

    cb.assert_not_called()
    uninit()
    cb.assert_not_called()
    uninit2()
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition"


@pytest.mark.local
def test_sequential(tmpd_cwd):
    """Test that if a log context can be released completely, and then reinitialized."""
    lc = Mock()
    lc.uuid = uuid.uuid4()

    assert b._initialized_log_contexts == {}, "precondition"

    uninit1 = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_1")
    lc.initialize_logging.assert_called_once()
    cb = b._initialized_log_contexts[lc.uuid].uninit_callback
    cb.assert_not_called()
    uninit1()
    cb.assert_called()

    assert b._initialized_log_contexts == {}, "back to satisfying the precondition, between uses"

    uninit2 = b.oneshot_initialize_logging(log_config=lc, log_dir=tmpd_cwd, log_name="test_nested_2")
    assert lc.initialize_logging.call_count == 2, "Should have been initialized once per block"

    cb2 = b._initialized_log_contexts[lc.uuid].uninit_callback

    # There is no requirement that cb2 be a fresh callback (mock) or be re-used.
    # When writing this test, cb2 is a reused mock, `cb2 is cb`, but that isn't
    # a requirement of the API.
    c = cb2.call_count
    uninit2()
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
    uninit = b.oneshot_initialize_logging(log_config=pickle.loads(p), log_dir=tmpd_cwd, log_name="test_nested_1")

    assert lc_orig.uuid in b._initialized_log_contexts
    uninit2 = b.oneshot_initialize_logging(log_config=pickle.loads(p), log_dir=tmpd_cwd, log_name="test_nested_2")

    assert lc_orig.uuid in b._initialized_log_contexts

    uninit2()
    assert lc_orig.uuid in b._initialized_log_contexts

    uninit()
    assert lc_orig.uuid not in b._initialized_log_contexts
