import pytest

import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors import WorkQueueExecutor
from parsl.executors.errors import InvalidResourceSpecification
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.executors.threads import ThreadPoolExecutor


@python_app
def double(x, parsl_resource_specification={}):
    return x * 2


@pytest.mark.issue_3620
def test_resource(n=2):
    executors = parsl.dfk().executors
    executor = None
    for label in executors:
        if label != '_parsl_internal':
            executor = executors[label]
            break

    # Specify incorrect number of resources
    # "disk" is missing
    spec = {'cores': 1, 'memory': 1}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except InvalidResourceSpecification:
        assert (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))
    else:
        assert not (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))

    # Specify resources with wrong types
    # 'cpus' is incorrect, should be 'cores'
    spec = {'cpus': 1, 'memory': 1, 'disk': 1}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except InvalidResourceSpecification:
        assert (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))
    else:
        assert not (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))


@python_app
def long_delay(parsl_resource_specification={}):
    import time
    time.sleep(30)


@pytest.mark.skip('I need to understand whats happening here better')
@pytest.mark.local
def test_wq_resource_excess():
    c = Config(executors=[WorkQueueExecutor(port=9000, enable_monitoring=True)])

    parsl.load(c)
    f = long_delay(parsl_resource_specification={'memory': 1, 'disk': 1, 'cores': 1})
    assert f.exception() is not None, "This should have failed"
