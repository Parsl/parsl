import pytest
from parsl.jobs.states import JobState


@pytest.mark.local
def test_JobStatus_repr():
    # in Python 3.11, the behavior of enums changed a bit, and so repr
    # (inherited from a superclass) raised an exception rather than
    # returning a string.

    # This test checks that repr returns some string value - it does
    # not validate that that string is some expected value.

    j = JobState.COMPLETED

    # this repr call started raising exceptions with python 3.11
    # prior to the #2652 bugfix.
    r = repr(j)
    assert isinstance(r, str)

    s = str(j)
    assert isinstance(s, str)

    # IntEnum changed behaviour from python 3.10 -> 3.10 so as to return
    # an integer. This checks that the JobState enum implementation
    # returns a name string on all tested platforms.
    assert s == "JobState.COMPLETED"
