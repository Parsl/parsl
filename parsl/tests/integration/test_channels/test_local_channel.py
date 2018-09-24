from parsl.channels.local.local import LocalChannel


def test_env():
    ''' Regression testing for issue #27
    '''

    lc = LocalChannel()
    rc, stdout, stderr = lc.execute_wait("env", 1)

    stdout = stdout.split('\n')
    x = [l for l in stdout if l.startswith("PATH=")]
    assert x, "PATH not found"

    x = [l for l in stdout if l.startswith("HOME=")]
    assert x, "HOME not found"

    print("RC:{} \nSTDOUT:{} \nSTDERR:{}".format(rc, stdout, stderr))


def test_env_mod():
    ''' Testing for env update at execute time.
    '''

    lc = LocalChannel()
    rc, stdout, stderr = lc.execute_wait("env", 1, {'TEST_ENV': 'fooo'})

    stdout = stdout.split('\n')
    x = [l for l in stdout if l.startswith("PATH=")]
    assert x, "PATH not found"

    x = [l for l in stdout if l.startswith("HOME=")]
    assert x, "HOME not found"

    x = [l for l in stdout if l.startswith("TEST_ENV=fooo")]
    assert x, "User set env missing"


if __name__ == "__main__":

    test_env()
    test_env_mod()
