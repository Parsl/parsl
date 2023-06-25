import os
import pytest

import parsl
from parsl import bash_app
from parsl.tests.configs.local_threads import fresh_config


def half_handler(*args):
    """Cost 0.5 for each retry, not the default of 1"""
    return 0.5


def local_config():
    c = fresh_config()
    c.retries = 2
    c.retry_handler = half_handler
    return c


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


@pytest.mark.local
def test_retry():
    """Test retries via app that succeeds on the Nth retry.
    """

    fname = "retry.out"
    try:
        os.remove(fname)
    except OSError:
        pass
    fu = succeed_on_retry(fname, success_on=4)

    fu.result()

    try:
        os.remove(fname)
    except OSError:
        pass
    fu = succeed_on_retry(fname, success_on=5)

    with pytest.raises(parsl.app.errors.BashExitFailure):
        fu.result()

    assert fu.exception().exitcode == 5
