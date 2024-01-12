import pytest

from parsl.utils import RepresentationMixin


class GoodRepr(RepresentationMixin):
    def __init__(self, x, y):
        self.x = x
        self.y = y


class BadRepr(RepresentationMixin):
    """This class incorrectly subclasses RepresentationMixin.
    It does not store the parameter x on self.
    """
    def __init__(self, x, y):
        self.y = y


@pytest.mark.local
def test_repr_good():
    p1 = "parameter 1"
    p2 = "the second parameter"

    # repr should not raise an exception
    r = repr(GoodRepr(p1, p2))

    # representation should contain both values supplied
    # at object creation.
    assert p1 in r
    assert p2 in r


@pytest.mark.local
def test_repr_bad():
    p1 = "parameter 1"
    p2 = "the second parameter"

    # repr should raise an exception
    with pytest.raises(AttributeError):
        repr(BadRepr(p1, p2))


class NonValidatingRepresentationMixin(RepresentationMixin):
    """This will override the process level RepresentationMixin which can
    be set to validating mode by pytest fixtures"""
    _validate_repr = False


class BadReprNonValidating(NonValidatingRepresentationMixin):
    """This class incorrectly subclasses RepresentationMixin.
    It does not store the parameter x on self.
    """
    def __init__(self, x, y):
        self.y = y


@pytest.mark.local
def test_repr_bad_unvalidated():
    p1 = "parameter 1"
    p2 = "the second parameter"

    # repr should not raise an exception
    r = repr(BadReprNonValidating(p1, p2))
    # parameter 2 should be found in the representation, but not
    # parameter 1
    assert p1 not in r
    assert p2 in r
