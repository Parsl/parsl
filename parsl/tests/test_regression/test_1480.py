import parsl
from parsl import python_app
import pytest
from parsl.tests.configs.htex_local import config

local_config = config


@python_app
def compute_descript(size=1000):
    import numpy as np
    x = np.array(list(range(0, size)), dtype=complex).astype(np.float32)
    return x


@pytest.mark.local
def test_1480(size=10**6):
    #parsl.load(config)
    x = compute_descript(size=size)

    # This should raise a TypeError: can't pickle memoryview objects
    with pytest.raises(TypeError):
        x.result()
    #parsl.clear()


if __name__ == "__main__":
    test_1480()
