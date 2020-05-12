from parsl.providers.provider_base import JobState, JobStatus


def test_eq():
    """ Regression test 1 for #1682
    """
    assert JobStatus(JobState.RUNNING) == JobStatus(JobState.RUNNING), "Failing jobstatus equality"


def test_ne():
    """ Regression test 2 for #1682
    """
    assert JobStatus(JobState.FAILED) != JobStatus(JobState.RUNNING), "Failing inequality check"


if __name__ == "__main__":

    test_eq()
    test_ne()
