import pytest

from parsl import python_app
from parsl.tests.configs.htex_local import fresh_config as local_config


@python_app
def compute_descript(size=1000):
    import numpy as np
    x = np.array(list(range(0, size)), dtype=complex).astype(np.float32)
    return x


@pytest.mark.local
def test_1653():
    """ Check if #1653 works correctly
    """
    x = compute_descript(size=100).result()
    assert x.shape == (100,), "Got incorrect numpy shape"

    x = compute_descript(size=1000).result()
    assert x.shape == (1000,), "Got incorrect numpy shape"
