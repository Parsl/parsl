import parsl
from parsl import App, python_app
import pytest

# dfk = parsl.load()

@python_app
def cpu_stress(inputs=[], outputs=[]):
    s = 0
    for i in range(10**8):
        s += i
    return s

@pytest.mark.noci
def test_parsl_load_default_config():
    a1, b1 = [cpu_stress(),
              cpu_stress()]
    a1.result()
    b1.result()
    # dfk.cleanup()


if __name__ == '__main__':
    test_parsl_load_default_config()
