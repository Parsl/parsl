import pytest

from parsl.channels.local.local import execute_wait


@pytest.mark.local
def test_env():
    ''' Regression testing for issue #27
    '''

    rc, stdout, stderr = execute_wait("env", 1)

    stdout = stdout.split('\n')
    x = [s for s in stdout if s.startswith("PATH=")]
    assert x, "PATH not found"

    x = [s for s in stdout if s.startswith("HOME=")]
    assert x, "HOME not found"

    print("RC:{} \nSTDOUT:{} \nSTDERR:{}".format(rc, stdout, stderr))
