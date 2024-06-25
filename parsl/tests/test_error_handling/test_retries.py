import os

import pytest

from parsl import bash_app, python_app
from parsl.tests.configs.local_threads import fresh_config


def local_config():
    c = fresh_config()
    c.retries = 2
    return c


@python_app
def sleep_then_fail(inputs=[], sleep_dur=0.1):
    import math
    import time
    time.sleep(sleep_dur)
    math.ceil("Trigger TypeError")
    return 0


@bash_app
def succeed_on_retry(filename, success_on=1, stdout="succeed.out"):
    """If the input file does not exist it creates it.
    Then, if the file contains success_on lines it exits with 0
    """

    return """if [[ ! -e {filename} ]]; then touch {filename}; fi;
    tries=`wc -l {filename} | cut -f1 -d' '`
    echo $tries >> {filename}

    if [[ "$tries" -eq "{success_on}" ]]
    then
        echo "Match. Success"
    else
        echo "Tries != success_on , exiting with error"
        exit 5
    fi
    """.format(filename=filename, success_on=success_on)


@python_app
def sleep(sleep_dur=0.1):
    import time
    time.sleep(sleep_dur)
    return 0


@pytest.mark.local
def test_fail_nowait(numtasks=10):
    """Test retries on tasks with no dependencies.
    """
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(sleep_dur=0.1)
        fus.extend([fu])

    # wait for all tasks to complete before ending this test
    [x.exception() for x in fus]

    try:
        [x.result() for x in fus]
    except Exception as e:
        assert isinstance(
            e, TypeError), "Expected a TypeError, got {}".format(e)


@pytest.mark.local
def test_fail_delayed(numtasks=10):
    """Test retries on tasks with dependencies.

    This is testing retry behavior when AppFutures are created
    with no parent.
    """

    x = sleep()
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(inputs=[x], sleep_dur=0.5)
        fus.extend([fu])

    # wait for all tasks to complete before ending this test
    [x.exception() for x in fus]

    try:
        [x.result() for x in fus]
    except Exception as e:
        assert isinstance(
            e, TypeError), "Expected a TypeError, got {}".format(e)


@pytest.mark.local
def test_retry(tmpd_cwd):
    """Test retries via app that succeeds on the Nth retry.
    """

    fpath = tmpd_cwd / "retry.out"
    sout = str(tmpd_cwd / "stdout")
    succeed_on_retry(str(fpath), stdout=sout).result()
