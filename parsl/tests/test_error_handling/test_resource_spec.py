import parsl
import pytest

from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors.errors import UnsupportedFeatureError, ExecutorError
from parsl.executors import WorkQueueExecutor


@python_app
def double(x, parsl_resource_specification={}):
    return x * 2


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
    except UnsupportedFeatureError:
        assert not isinstance(executor, WorkQueueExecutor)
    except Exception as e:
        assert isinstance(e, ExecutorError)

    # Specify resources with wrong types
    # 'cpus' is incorrect, should be 'cores'
    spec = {'cpus': 2, 'memory': 1000, 'disk': 1000}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except UnsupportedFeatureError:
        assert not isinstance(executor, WorkQueueExecutor)
    except Exception as e:
        assert isinstance(e, ExecutorError)


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
