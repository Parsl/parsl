import parsl

from parsl.app.app import App
from parsl.tests.configs.local_threads import config

parsl.clear()
parsl.load(config)


@App('bash')
def echo(message, outputs=[]):
    return 'echo {0} &> {outputs[0]}'


@App('python')
def cat(inputs=[]):
    with open(inputs[0].filepath) as f:
        return f.readlines()


def test_slides():
    """Testing code snippet from slides """

    hello = echo("Hello World!", outputs=['hello1.txt'])
    message = cat(inputs=[hello.outputs[0]])

    # Waits. This need not be in the slides.
    print(hello.result())
    print(message.result())
