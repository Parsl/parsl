from parsl.channels.local.local import LocalChannel


def test_local():

    channel = LocalChannel(None, None)

    ec, out, err = channel.execute_wait('echo "pwd: $PWD"', 2)

    assert ec == 0, "Channel execute failed"
    print("Stdout: ", out)
    print("Stderr: ", err)


if __name__ == "__main__":

    test_local()
