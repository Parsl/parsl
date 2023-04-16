import pytest
from parsl.providers.base import JobState

@pytest.mark.local
def test_JobStatus_repr():
    # in Python 3.11, the behavior of enums changed a bit, and so repr
    # (inherited from a superclass) raised an exception rather than
    # returning a string.

    # This test checks that repr returns some string value - it does
    # not validate that that string is some expected value.

    j = JobState.COMPLETED

    r = repr(j)
    assert isinstance(r, str)

    s = str(j)
    assert isinstance(s, str)
