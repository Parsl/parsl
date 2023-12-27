import pytest

from parsl.providers import SlurmProvider


@pytest.mark.local
def test_slurm_instantiate_regression_2994():
    """This test checks that repr can be executed on SlurmProvider.
    This does not need a SLURM installation around.

    Because of the behaviour of RepresentationMixin, it's a relatively
    common problem to break repr when adding new parameters to the
    SlurmProvider, and this tests that repr does not raise an exception.
    """
    p = SlurmProvider()
    repr(p)
