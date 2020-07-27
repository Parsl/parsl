import parsl
import pytest
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config
from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors import WorkQueueExecutor


@python_app
def double(x, parsl_resource_specification={}):
    return x * 2


@pytest.mark.skip("this test does not accomodate running the test suite"
                  " on executors which *do* support resource specifications"
                  " but are not the workqueue executor. In general, it is"
                  " incorrect to assume that an arbitrary non-workqueue"
                  " executor will raise the expected exceptionm")
def test_resource(n=2):
    spec = {'cores': 2, 'memory': '1GiB'}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except Exception as e:
        assert isinstance(e, UnsupportedFeatureError)
    else:
        executors = parsl.dfk().executors
        executor = None
        for label in executors:
            if label != 'data_manager':
                executor = executors[label]
                break
        assert isinstance(executor, WorkQueueExecutor)


if __name__ == '__main__':
    local_config = config
    parsl.load(local_config)
    x = test_resource(2)
