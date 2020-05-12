import parsl
import pytest
from parsl import python_app
from parsl.tests.configs.local_threads import fresh_config


@pytest.mark.local
def test_lazy_behavior():
    """Testing that lazy errors work"""

    config = fresh_config()
    parsl.load(config)

    @python_app
    def divide(a, b):
        return a / b

    futures = []
    for i in range(0, 10):
        futures.append(divide(10, 0))

    for f in futures:
        assert isinstance(f.exception(), ZeroDivisionError)
        assert f.done()

    parsl.clear()
    return


if __name__ == "__main__":

    test_lazy_behavior()
