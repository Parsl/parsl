import parsl
import pytest


@parsl.python_app
def identity(x):
    return x


def test_identity():
    assert identity(7).result() == 7

    assert identity([1, 2, 3]).result() == [1, 2, 3]

    assert id(identity([1, 2, 3]).result()) != id([1, 2, 3])


@parsl.python_app
def return_range(x):
    return list(range(x))


@pytest.mark.skip("this demonstrates an issue, by failing")
def test_range_identities():
    # these don't necessarily make sense with caching turned on so beware.
    x = 3

    fut1 = return_range(x)
    res1 = fut1.result()

    fut2 = return_range(x)
    res2 = fut2.result()

    assert id(fut1) != id(fut2), "The two app invocations should return different futures"

    assert res1 == res2, "The two app invocations should return equal results"
    assert id(res1) != id(res2), "The two app invocations should return different objects"

    res1.append(4)
    assert res1 != res2, "Mutating one result should not mutate the other"
