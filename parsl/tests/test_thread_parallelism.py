import time

import pytest

from parsl.app.app import bash_app, python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def sleep_python(x):
    import time
    time.sleep(x)
    return x


@bash_app
def sleep_bash(x):
    return 'sleep {0}'


@pytest.mark.local
@pytest.mark.skip('fails intermittently in pytest')
def test_parallel_sleep_bash(n=3, sleep_dur=2, tolerance=0.3):
    """Indirect test to ensure that 10 threads are live using bash apps
    """

    start = time.time()

    d = []
    for i in range(0, n):
        d.extend([sleep_bash(sleep_dur)])

    for i in d:
        i.result()
    end = time.time()
    delta = end - start
    print("Sleep time : {0}, expected ~{1}+/- 0.3s".format(delta, sleep_dur))
    assert delta > sleep_dur - tolerance, "Slept too little"
    assert delta < sleep_dur + tolerance, "Slept too much"


@pytest.mark.local
@pytest.mark.skip('fails intermittently in pytest')
def test_parallel_sleep_python(n=3, sleep_dur=2, tolerance=0.3):
    """Indirect test to ensure that 10 threads are live using python sleep apps
    This works only because the 10 threads are essentially sleeping and not
    doing manipulation of python objects and causing serialization via GIL.
    """

    start = time.time()

    d = []
    for i in range(0, n):
        d.extend([sleep_python(sleep_dur)])

    for i in d:
        i.result()
    end = time.time()
    delta = end - start
    print("Sleep time : {0}, expected ~{1}+/- 0.3s".format(delta, sleep_dur))
    assert delta > sleep_dur - tolerance, "Slept too little"
    assert delta < sleep_dur + tolerance, "Slept too much"
