from parsl.channels import OAuthSSHChannel
import pytest

@pytest.mark.noci
def test_channel():
    channel = OAuthSSHChannel(hostname='ssh.demo.globus.org', username='yadunand')
    x, stdout, stderr = channel.execute_wait('ls')
    print(x, stdout, stderr)
    assert x == 0, "Expected exit code 0, got {}".format(x)


if __name__ == '__main__':

    test_channel()
