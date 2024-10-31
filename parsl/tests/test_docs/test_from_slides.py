import os

import pytest

from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File


@bash_app
def echo(message, outputs=[]):
    return 'echo {m} &> {o}'.format(m=message, o=outputs[0])


@python_app
def cat(inputs=[]):
    with open(inputs[0].filepath) as f:
        return f.readlines()


@pytest.mark.staging_required
def test_slides():
    """Testing code snippet from slides """

    if os.path.exists('hello1.txt'):
        os.remove('hello1.txt')

    hello = echo("Hello World!", outputs=[File('hello1.txt')])

    message = cat(inputs=[hello.outputs[0]])

    # Waits. This need not be in the slides.
    print(hello.result())
    print(message.result())
