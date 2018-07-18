import pytest
from parsl import App, File
import parsl
from parsl.configs.local_threads import config

parsl.clear()
parsl.load(config)


@App('bash')
def cat(inputs=[], stdout='stdout.txt'):
    return 'cat %s' % (inputs[0])


@pytest.mark.local
def test():
    # create a test file
    open('/tmp/test.txt', 'w').write('Hello\n')

    # create the Parsl file
    parsl_file = File('file:///tmp/test.txt')

    # call the cat app with the Parsl file
    cat(inputs=[parsl_file])
