import pytest

from parsl.channels.local.local import LocalChannel


@pytest.mark.local
def test_local_large_output_2210():
    """Regression test for #2210.
    The local channel was hanging if the specified command gave too
    much output, due to a race condition between process exiting and
    pipes filling up.
    """

    c = LocalChannel()

    # this will output 128kb of stdout
    c.execute_wait("yes | dd count=128 bs=1024", walltime=60)

    # if this test fails, execute_wait should raise a timeout
    # exception.

    # The contents out the output is not verified by this test
