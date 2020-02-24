import pytest

from parsl.app.app import python_app
from parsl.tests.configs.htex_local import config

local_config = config


@python_app
def slow_double(x, dur=0.1):
    import time
    time.sleep(dur)
    return x * 5


@pytest.mark.local
def test_cleanup_behavior_221():
    """ A1 A2 A3   -> cleanup
        B1 B2 B3

    """

    round_1 = []
    for i in range(0, 2):
        f = slow_double(i)
        round_1.append(f)

    round_2 = []
    for i in round_1:
        f = slow_double(i)
        round_2.append(f)


if __name__ == "__main__":

    test_cleanup_behavior_221()
