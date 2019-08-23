import parsl
from parsl.channels.ssh.ssh import SSHChannel as SSH
import pytest

def connect_and_list(hostname, username):
    conn = SSH(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out

@pytest.mark.skip('manual run only')
def test_push(conn, fname="test001.txt"):

    with open(fname, 'w') as f:
        f.write("Hello from parsl.ssh testing\n")

    conn.push_file(fname, "/tmp")
    ec, out, err = conn.execute_wait("ls /tmp/{0}".format(fname))
    print(ec, out, err)

@pytest.mark.skip('manual run only')
def test_pull(conn, fname="test001.txt"):

    local = "foo"
    conn.pull_file("/tmp/{0}".format(fname), local)

    with open("{0}/{1}".format(local, fname), 'r') as f:
        print(f.readlines())


if __name__ == "__main__":

    parsl.set_stream_logger()

    # This is for testing
    conn = SSH("midway.rcc.uchicago.edu", username="yadunand")

    test_push(conn)
    test_pull(conn)

    conn.close()
