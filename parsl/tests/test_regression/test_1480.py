import pytest

from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config as local_config


@python_app
def compute_descript(size=1000):
    import numpy as np
    x = np.array(list(range(0, size)), dtype=complex).astype(np.float32)
    return x


@pytest.mark.local
def test_1480(size=10**6):
    x = compute_descript(size=size)

    # Before PR#1841 this would have raised a TypeError
    # Now, with the threshold increased this should not trigger any error
    assert len(x.result()) == size, "Lengths do not match"
