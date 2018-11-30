import parsl
import pytest
from parsl import App
from parsl.tests.configs.local_threads import config


@pytest.mark.local
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_non_lazy_behavior():
    """Testing non lazy errors to work"""

    parsl.clear()
    config.lazy_errors = False
    parsl.load(config)

    @App('python')
    def divide(a, b):
        return a / b

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
