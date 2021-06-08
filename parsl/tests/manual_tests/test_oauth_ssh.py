from parsl.channels import OAuthSSHChannel


def test_channel():
    channel = OAuthSSHChannel(hostname='ssh.demo.globus.org', username='yadunand')
    x, stdout, stderr = channel.execute_wait('ls')
    print(x, stdout, stderr)
    assert x == 0, f"Expected exit code 0, got {x}"


if __name__ == '__main__':

    test_channel()
