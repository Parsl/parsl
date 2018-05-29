import parsl
import pytest
from parsl import App
from parsl.tests.configs.local_threads import config
config['globals']['lazyErrors'] = True
parsl.clear()
parsl.load(config)


@App('python')
def divide(a, b):
    return a / b


@pytest.mark.local
def test_lazy_behavior():
    """Testing non lazy errors to work"""

    items = []
    for i in range(0, 1):
        items.append(divide(10, i))

    while True:
        if items[0].done:
            break

    return


if __name__ == "__main__":

    test_lazy_behavior()
