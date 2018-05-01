"""Testing bash apps
"""
import parsl
from parsl import *

print("Parsl version: ", parsl.__version__)


# parsl.set_stream_logger()
DataFlowKernelLoader.set_default('configs/local_threads.py')
dfk = DataFlowKernelLoader.dfk()


@App('python', dfk)
def generate(limit):
    from random import randint
    """Generate a random integer and return it"""
    return randint(1, limit)


@App('bash', dfk)
def save(message, outputs=[]):
    return 'echo %s &> {outputs[0]}' % (message)


def test_procedural(N=4):
    """Procedural workflow example from docs on
    Composing a workflow
    """
    message = generate(N)

    saved = save(message, outputs=['output.txt'])

    with open(saved.outputs[0].result().filepath, 'r') as f:
        item = int(f.read().strip())
        assert item <= N, "Expected file to contain int <= N"
        assert item >= 1, "Expected file to contain int >= 1"


if __name__ == "__main__":

    test_procedural()
