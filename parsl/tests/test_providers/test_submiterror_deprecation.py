import random
import string

import pytest

from parsl.providers.errors import SubmitException


@pytest.mark.local
def test_submit_exception_task_name_deprecation():
    """This tests the deprecation warning of task_name in SubmitException
    """
    j = "the_name-" + "".join(random.sample(string.ascii_lowercase, 10))

    ex = SubmitException(j, "m")

    # the new behaviour
    assert ex.job_name == j

    # the old behaviour
    with pytest.deprecated_call():
        assert ex.task_name == j
