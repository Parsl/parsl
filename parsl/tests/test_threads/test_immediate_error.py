import parsl
import pytest
from parsl import App
from parsl.dataflow.dflow import DataFlowKernel
from parsl.tests.configs.local_threads import config
config['globals']['lazyErrors'] = False
parsl.clear()
dfk = DataFlowKernel(config=config)


@App('python', dfk)
def divide(a, b):
    return a / b


@pytest.mark.local
def test_non_lazy_behavior():
    """Testing non lazy errors to work"""

    try:
        items = []
        for i in range(0, 1):
            items.append(divide(10, i))

        while True:
            if items[0].done:
                break

    except Exception as e:
        assert isinstance(e, ZeroDivisionError), "Expected ZeroDivisionError, got: {}".format(e)
    else:
        raise Exception("Expected ZeroDivisionError, got nothing")

    return


if __name__ == "__main__":

    test_non_lazy_behavior()
