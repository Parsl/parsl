from parsl.channels.errors import SSHException, BadHostKeyException
from parsl.channels.ssh.ssh import SSHChannel as SSH


def connect_and_list(hostname, username):
    conn = SSH(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def test_error_1():
    try:
        connect_and_list("bad.url.gov", "ubuntu")
    except Exception as e:
        assert type(e) is SSHException, "Expected SSException, got: {0}".format(e)


def test_error_2():
    try:
        connect_and_list("swift.rcc.uchicago.edu", "mango")
    except SSHException:
        print("Caught the right exception")
    else:
        raise Exception("Expected SSException, got: {0}".format(e))


def test_error_3():
    ''' This should work
    '''
    try:
        connect_and_list("edison.nersc.gov", "yadunand")
    except BadHostKeyException as e:
        print("Caught exception BadHostKeyException: ", e)
    else:
        assert False, "Expected SSException, got: {0}".format(e)


if __name__ == "__main__":

    tests = [test_error_1, test_error_2, test_error_3]

    for test in tests:
        print("---------Running : {0}---------------".format(test))
        test()
        print("----------------------DONE--------------------------")
