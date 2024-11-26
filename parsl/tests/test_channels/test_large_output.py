import pytest

from parsl.utils import execute_wait


@pytest.mark.local
def test_local_large_output_2210():
    """Regression test for #2210.
    execute_wait was hanging if the specified command gave too
    much output, due to a race condition between process exiting and
    pipes filling up.
    """

    # this will output 128kb of stdout
    execute_wait("yes | dd count=128 bs=1024", walltime=60)

    # if this test fails, execute_wait should raise a timeout
    # exception.

    # The contents out the output is not verified by this test
