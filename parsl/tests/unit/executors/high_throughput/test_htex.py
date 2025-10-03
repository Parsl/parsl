from unittest import mock

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
