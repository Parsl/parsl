import parsl
import pytest
from parsl import App
import time
from parsl.tests.configs.local_threads import config
config['globals']['lazyErrors'] = False
parsl.load(config)


@App('python')
def divide(a, b):
    return a / b


@pytest.mark.local
def test_non_lazy_behavior():
    """Testing non lazy errors to work"""

    try:
        items = []
        for i in range(0, 10):
            items.append(divide(10, i))
        time.sleep(1)
    except Exception as e:
        assert isinstance(e, ZeroDivisionError), "Expected ZeroDivisionError, got : {}".format(e)
    else:
        raise("Expected ZeroDivisionError, got Nothing")

    return


if __name__ == "__main__":

    test_non_lazy_behavior()
