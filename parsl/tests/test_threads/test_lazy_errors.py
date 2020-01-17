import parsl
import pytest
from parsl import python_app
from parsl.tests.configs.local_threads import fresh_config


@pytest.mark.local
def test_lazy_behavior():
    """Testing that lazy errors work"""

    config = fresh_config()
    config.lazy_errors = True
    parsl.load(config)

    @python_app
    def divide(a, b):
        return a / b

    future = divide(10, 0)

    while not future.done():
        pass

    parsl.clear()
    return


if __name__ == "__main__":

    test_lazy_behavior()
