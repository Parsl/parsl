import pytest

from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File


@bash_app
def echo(message, outputs=()):
    return 'echo -n {m} &> {o}'.format(m=message, o=outputs[0])


@python_app
def cat(inputs=()):
    with open(inputs[0].filepath) as f:
        return f.read()


@pytest.mark.staging_required
def test_slides(tmpd_cwd, randomstring):
    """Testing code snippet from slides """

    exp_msg = f"Hello World! -- {randomstring()}"
    outputs = [File(tmpd_cwd / "hello1.txt")]

    hello = echo(exp_msg, outputs=outputs)

    message = cat(inputs=[hello.outputs[0]])

    assert message.result() == exp_msg
    assert hello.result() == 0
