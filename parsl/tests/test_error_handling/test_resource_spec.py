import pytest

import parsl
from parsl.app.app import python_app
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
    spec = {'cores': 2, 'memory': 1000}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except InvalidResourceSpecification:
        assert (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))

    # Specify resources with wrong types
    # 'cpus' is incorrect, should be 'cores'
    spec = {'cpus': 2, 'memory': 1000, 'disk': 1000}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except InvalidResourceSpecification:
        assert (
            isinstance(executor, HighThroughputExecutor) or
            isinstance(executor, WorkQueueExecutor) or
            isinstance(executor, ThreadPoolExecutor))
