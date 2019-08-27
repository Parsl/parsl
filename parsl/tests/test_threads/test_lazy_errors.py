import parsl
import pytest
from parsl import App
from parsl.tests.configs.local_threads import fresh_config


@pytest.mark.local
def test_lazy_behavior():
    """Testing lazy errors to work"""

    config = fresh_config()
    config.lazy_errors = True
    parsl.load(config)

    @App('python')
    def divide(a, b):
        return a / b

    items = []
    for i in range(0, 1):
        items.append(divide(10, i))

    while True:
        if items[0].done:
            break

    parsl.clear()
    return


if __name__ == "__main__":

    test_lazy_behavior()
