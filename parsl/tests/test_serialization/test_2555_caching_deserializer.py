import pytest

import parsl
from parsl.tests.configs.htex_local import fresh_config as local_config


@parsl.python_app
def return_range(x):
    return list(range(x))


@pytest.mark.local
def test_range_identities():
    x = 3

    fut1 = return_range(x)
    res1 = fut1.result()

    fut2 = return_range(x)
    res2 = fut2.result()

    # Check that the returned futures are different, by both usual
    # Python equalities.
    # This is not strictly part of the regression test for #2555
    # but will detect related unexpected Future caching.

    assert fut1 != fut2
    assert id(fut1) != id(fut2)

    # check that the two invocations returned the same value...
    assert res1 == res2

    # ... but in two different objects.
    assert id(res1) != id(res2)
