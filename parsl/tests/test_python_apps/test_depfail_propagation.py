from parsl import python_app
from parsl.dataflow.error import DependencyError


@python_app
def fails():
    raise ValueError("Deliberate failure")


@python_app
def depends(parent):
    return 1


def test_depfail_once():
    """Test the simplest dependency failure case"""
    f1 = fails()
    f2 = depends(f1)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)


def test_depfail_chain():
    """Test that dependency failures chain"""
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f2)
    f4 = depends(f3)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)
    assert isinstance(f3.exception(), DependencyError)
    assert isinstance(f4.exception(), DependencyError)


def test_depfail_branches():
    """Test that dependency failures propagate in the
    presence of multiple downstream tasks."""

    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f1)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)
    assert isinstance(f3.exception(), DependencyError)
