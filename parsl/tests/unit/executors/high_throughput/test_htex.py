from unittest import mock

import msgspec
import pytest

from parsl import HighThroughputExecutor
from parsl.executors.high_throughput import zmq_pipes


@pytest.mark.local
def test_submit_payload():
    htex = HighThroughputExecutor()
    htex.outgoing_q = mock.Mock(spec=zmq_pipes.TasksOutgoing)
    ctxt = {"some": "context"}
    buf = b'some buffer (function) payload'
    for task_num in range(1, 20):
        htex.outgoing_q.reset_mock()
        fut = htex.submit_payload(ctxt, buf)
        (msg,), _ = htex.outgoing_q.put.call_args

        assert htex.tasks[fut.parsl_executor_task_id] is fut
        assert fut.parsl_executor_task_id == task_num, "Expect monotonic increase"
        assert msg["task_id"] == fut.parsl_executor_task_id
        assert msg["context"] == ctxt, "Expect no modification"
        assert msg["buffer"] == buf, "Expect no modification"


@pytest.mark.local
def test_submit_payload_passes_context_through_unconverted():
    htex = HighThroughputExecutor()
    htex.outgoing_q = mock.Mock(spec=zmq_pipes.TasksOutgoing)
    ctxt = {"task_executor": {"f": "a.b", "a": ("x", 1)}, "resource_spec": {"priority": 3}}
    htex.submit_payload(ctxt, b"")
    (msg,), _ = htex.outgoing_q.put.call_args
    assert msg["context"] is ctxt
    assert type(msg["context"]["resource_spec"]["priority"]) is int
    assert type(msg["context"]["task_executor"]["a"]) is tuple


@pytest.mark.local
@pytest.mark.parametrize("ctxt", (
    ["not", "a", "dict"],
    {"resource_spec": {"priority": "high"}},
    {"resource_spec": {"num_nodes": "2"}},
    {"task_executor": {"a": ()}},
    {"task_executor": {"f": "a.b", "k": ["not", "a", "dict"]}},
))
def test_submit_payload_rejects_ill_typed_context(ctxt):
    htex = HighThroughputExecutor()
    htex.outgoing_q = mock.Mock(spec=zmq_pipes.TasksOutgoing)
    with pytest.raises(msgspec.ValidationError):
        htex.submit_payload(ctxt, b"")
    assert not htex.tasks
    htex.outgoing_q.put.assert_not_called()
