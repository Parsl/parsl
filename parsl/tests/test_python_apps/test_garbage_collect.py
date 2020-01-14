import parsl
import gc
from parsl.app.app import python_app


@python_app
def slow_double(x):
    import time
    time.sleep(0.1)
    return x * 2


def test_garbage_collect():
    """ Launches an app with a dependency and waits till it's done and asserts that
    the internal refs were wiped
    """
    x = slow_double(slow_double(10))

    refs = gc.get_referrers(x)
    if x.done() is False:
        assert len(refs) > 1, "Expected >1 refs before done"

    x.result()
    refs = gc.get_referrers(x)
    assert len(refs) == 1, "Expected only 1 live reference from main context"


if __name__ == '__main__':

    parsl.load()

    test_garbage_collect()
