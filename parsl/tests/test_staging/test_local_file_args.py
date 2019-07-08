import pytest

import parsl
from parsl.data_provider.files import File

if __name__ == "__main__":
    parsl.set_stream_logger()
    # initialise logging before importing config, to get logging
    # from config declaration # as it looks like the AWS initializer
    # is doing more than just creating data structures

from parsl import bash_app
from parsl.tests.configs.local_threads_file_args import config

import logging
logger = logging.getLogger(__name__)


@bash_app
def bash_cat(inputs=[], outputs=[]):
    return 'cat {inp} > {out}'.format(inp=inputs[0].filepath, out=outputs[0].filepath)


def setup_module(module):
    parsl.load(config)


@pytest.mark.local
def test_bash():
    """Testing basic bash functionality."""

    # TODO: delete these files before use to stop
    # test overlap.

    in_file = File("test_bash.in")
    out_file = File("test_bash.out")

    x = bash_cat(inputs=[in_file], outputs=[out_file])

    # wait for the output future

    f = x.outputs[0].result()

    assert x.result() == 0, "Bash result code was not success"


if __name__ == "__main__":
    parsl.load(config)
    test_bash()
