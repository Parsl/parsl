import pytest

from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


local_config = fresh_config()
local_config.executors[0].init_blocks = 0


@python_app
def py_app():
    import platform
    return "Hello from {0}".format(platform.uname())


# @pytest.mark.local
@pytest.mark.skip("Broke somewhere between PR #525 and PR #652")
def test_python(N=2):
    """Testing basic scaling|Python 0 -> 1 block """

    results = {}
    for i in range(0, N):
        results[i] = py_app()

    print("Waiting ....")
    for i in range(0, N):
        print(results[0].result())


if __name__ == '__main__':

    test_python()
