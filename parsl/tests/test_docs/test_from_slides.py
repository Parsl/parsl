from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File

import os
import pytest


@bash_app
def echo(message, outputs=[]):
    return 'pwd > /tmp/xx2; echo {o} >> /tmp/xx2 ; echo {m} &> {o}'.format(m=message, o=outputs[0])


@python_app
def cat(inputs=[]):
    with open(inputs[0].filepath) as f:
        return f.readlines()


def test_slides():
    """Testing code snippet from slides """

    input_message = "Hello World!"

    if os.path.exists('hello1.txt'):
        os.remove('hello1.txt')

    hello = echo(input_message, outputs=[File('hello1.txt')])

    message = cat(inputs=[hello.outputs[0]])

    assert hello.exception() is None

    # this should exist as soon as hello is finished
    assert os.path.exists('hello1.txt')

    assert len(message.result()) == 1
    assert message.result()[0].strip() == input_message
