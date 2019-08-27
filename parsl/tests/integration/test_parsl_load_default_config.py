import parsl
from parsl import python_app


@python_app
def cpu_stress(inputs=[], outputs=[]):
    s = 0
    for i in range(10**8):
        s += i
    return s


def test_parsl_load_default_config():
    dfk = parsl.load()
    a1, b1 = [cpu_stress(),
              cpu_stress()]
    a1.result()
    b1.result()
    dfk.cleanup()


if __name__ == '__main__':
    test_parsl_load_default_config()
