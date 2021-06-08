import parsl
from parsl.channels.ssh.ssh import SSHChannel as SSH


def connect_and_list(hostname, username):
    conn = SSH(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def test_push(conn_, fname="test001.txt"):

    with open(fname, 'w') as f:
        f.write("Hello from parsl.ssh testing\n")

    conn_.push_file(fname, "/tmp")
    ec, out, err = conn_.execute_wait(f"ls /tmp/{fname}")
    print(ec, out, err)


def test_pull(conn_, fname="test001.txt"):

    local = "foo"
    conn_.pull_file(f"/tmp/{fname}", local)

    with open(f"{local}/{fname}", 'r') as f:
        print(f.readlines())


if __name__ == "__main__":

    parsl.set_stream_logger()

    # This is for testing
    conn = SSH("midway.rcc.uchicago.edu", username="yadunand")

    test_push(conn)
    test_pull(conn)

    conn.close()
