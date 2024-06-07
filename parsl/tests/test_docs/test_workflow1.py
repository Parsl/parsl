import os

import pytest

import parsl
from parsl.app.app import bash_app, python_app
from parsl.data_provider.files import File
from parsl.tests.configs.local_threads import config

# parsl.set_stream_logger()


@python_app
def generate(limit):
    from random import randint
    """Generate a random integer and return it"""
    return randint(1, limit)


@bash_app
def save(message, outputs=[]):
    return 'echo {m} &> {o}'.format(m=message, o=outputs[0])


@pytest.mark.staging_required
def test_procedural(N=2):
    """Procedural workflow example from docs on
    Composing a workflow
    """

    if os.path.exists('output.txt'):
        os.remove('output.txt')

    message = generate(N)

    saved = save(message, outputs=[File('output.txt')])

    with open(saved.outputs[0].result().filepath, 'r') as f:
        item = int(f.read().strip())
        assert item <= N, "Expected file to contain int <= N"
        assert item >= 1, "Expected file to contain int >= 1"
