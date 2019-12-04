import parsl
from parsl.configs.htex_local import config
from parsl import python_app
import pytest


@python_app
def compute_descript(size=1000):
    import numpy as np
    x = np.array(list(range(0, size)), dtype=complex).astype(np.float32)
    return x


@pytest.mark.local
def test_1480(size=1000):
    parsl.load(config)
    x = compute_descript(size=size)

    assert len(x.result()) == size, "Expected len:{} instead got: {}".format(size,
                                                                             len(x.result()))


if __name__ == "__main__":
    test_1480()
