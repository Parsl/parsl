import random
from unittest import mock

import pytest
from globus_compute_sdk import Executor

from parsl.executors import GlobusComputeExecutor


@pytest.fixture
def mock_ex():
    # Not Parsl's job to test GC's Executor
    yield mock.Mock(spec=Executor)


@pytest.mark.local
def test_gc_executor_mock_spec(mock_ex):
    # a test of tests -- make sure we're using spec= in the mock
    with pytest.raises(AttributeError):
        mock_ex.aasdf()


@pytest.mark.local
def test_gc_executor_label_default(mock_ex):
    gce = GlobusComputeExecutor(mock_ex)
    assert gce.label == type(gce).__name__, "Expect reasonable default label"


@pytest.mark.local
def test_gc_executor_label(mock_ex, randomstring):
    exp_label = randomstring()
    gce = GlobusComputeExecutor(mock_ex, label=exp_label)
    assert gce.label == exp_label


@pytest.mark.local
def test_gc_executor_resets_spec_after_submit(mock_ex, randomstring):
    submit_res = {randomstring(): "some submit res"}
    res = {"some": randomstring(), "spec": randomstring()}
    mock_ex.resource_specification = res
    mock_ex.user_endpoint_config = None
    gce = GlobusComputeExecutor(mock_ex)

    fn = mock.Mock()
    orig_res = mock_ex.resource_specification
    orig_uep = mock_ex.user_endpoint_config

    def mock_submit(*a, **k):
        assert mock_ex.resource_specification == submit_res, "Expect set for submission"
        assert mock_ex.user_endpoint_config is None
    mock_ex.submit.side_effect = mock_submit

    gce.submit(fn, resource_specification=submit_res)

    assert mock_ex.resource_specification == orig_res
    assert mock_ex.user_endpoint_config is orig_uep


@pytest.mark.local
def test_gc_executor_resets_uep_after_submit(mock_ex, randomstring):
    uep_conf = randomstring()
    res = {"some": randomstring()}
    gce = GlobusComputeExecutor(mock_ex)

    fn = mock.Mock()
    orig_res = mock_ex.resource_specification
    orig_uep = mock_ex.user_endpoint_config

    def mock_submit(*a, **k):

        assert mock_ex.resource_specification == res, "Expect set for submission"
        assert mock_ex.user_endpoint_config == uep_conf, "Expect set for submission"
    mock_ex.submit.side_effect = mock_submit

    gce.submit(fn, resource_specification={"user_endpoint_config": uep_conf, **res})

    assert mock_ex.resource_specification == orig_res
    assert mock_ex.user_endpoint_config is orig_uep


@pytest.mark.local
def test_gc_executor_happy_path(mock_ex, randomstring):
    mock_fn = mock.Mock()
    args = tuple(randomstring() for _ in range(random.randint(0, 3)))
    kwargs = {randomstring(): randomstring() for _ in range(random.randint(0, 3))}

    gce = GlobusComputeExecutor(mock_ex)
    gce.submit(mock_fn, {}, *args, **kwargs)

    assert mock_ex.submit.called, "Expect proxying of args to underlying executor"
    found_a, found_k = mock_ex.submit.call_args
    assert found_a[0] is mock_fn
    assert found_a[1:] == args
    assert found_k == kwargs


@pytest.mark.local
def test_gc_executor_shuts_down_asynchronously(mock_ex):
    gce = GlobusComputeExecutor(mock_ex)
    gce.shutdown()
    assert mock_ex.shutdown.called
    a, k = mock_ex.shutdown.call_args
    assert k["wait"] is False
    assert k["cancel_futures"] is True
