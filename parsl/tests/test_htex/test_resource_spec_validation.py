import queue
from unittest import mock

import pytest

from parsl.executors import HighThroughputExecutor
from parsl.executors.errors import InvalidResourceSpecification


def double(x):
    return x * 2


@pytest.mark.local
def test_submit_calls_validate():

    htex = HighThroughputExecutor()
    htex.outgoing_q = mock.Mock(spec=queue.Queue)
    htex.validate_resource_spec = mock.Mock(spec=htex.validate_resource_spec)

    res_spec = {}
    htex.submit(double, res_spec, (5,), {})
    htex.validate_resource_spec.assert_called()


@pytest.mark.local
def test_resource_spec_validation():
    htex = HighThroughputExecutor()
    ret_val = htex.validate_resource_spec({})
    assert ret_val is None


@pytest.mark.local
def test_resource_spec_validation_bad_keys():
    htex = HighThroughputExecutor()

    with pytest.raises(InvalidResourceSpecification):
        htex.validate_resource_spec({"num_nodes": 2})
