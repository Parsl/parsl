import pytest

import parsl


@parsl.python_app
def noop():
    pass


@pytest.mark.local
def test_executor_binding_threads(caplog):
    """Tests executor binding for executor with no task IDs"""
    with parsl.load():
        fut = noop()
        fut.result()

    bindings = [e for e in caplog.records
                if hasattr(e, 'parsl.task') and
                hasattr(e, 'parsl.executor') and
                e.__dict__['parsl.task'] == fut.tid and
                e.__dict__['parsl.executor'] == 'threads'
                ]

    assert len(bindings) == 1, "expected exactly one task/executor binding"


@pytest.mark.local
def test_executor_binding_htex(caplog):
    """Tests executor binding for executor with task IDs"""
    LABEL = "htex_log_test"
    with parsl.load(parsl.Config(executors=[parsl.HighThroughputExecutor(label=LABEL)])):
        fut = noop()
        fut.result()

    bindings = [e for e in caplog.records
                if hasattr(e, 'parsl.task') and
                hasattr(e, 'parsl.executor') and
                hasattr(e, 'parsl.executor_task') and
                e.__dict__['parsl.task'] == fut.tid and
                e.__dict__['parsl.executor'] == LABEL and
                e.__dict__['parsl.executor_task'] == 1  # HTEX task IDs start at 1
                ]

    assert len(bindings) == 1, "expected exactly one task/executor binding"
