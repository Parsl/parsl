import pytest

from parsl.app.app import python_app
from parsl.dataflow.errors import DependencyError


class ManufacturedTestFailure(Exception):
    pass


@python_app
def random_fail(fail_prob: float, inputs=None):
    import random
    if random.random() < fail_prob:
        raise ManufacturedTestFailure("App failure")


def test_no_deps():
    """Test basic error handling, with no dependent failures
    """
    futs = [random_fail(1), random_fail(0), random_fail(0)]

    for f in futs:
        try:
            f.result()
        except ManufacturedTestFailure:
            pass


def test_fail_sequence_first():
    t1 = random_fail(fail_prob=1)
    t2 = random_fail(fail_prob=0, inputs=[t1])
    t_final = random_fail(fail_prob=0, inputs=[t2])

    with pytest.raises(DependencyError):
        t_final.result()

    assert len(t_final.exception().dependent_exceptions_tids) == 1
    assert isinstance(t_final.exception().dependent_exceptions_tids[0][0], DependencyError)
    assert t_final.exception().dependent_exceptions_tids[0][1].startswith("task ")


def test_fail_sequence_middle():
    t1 = random_fail(fail_prob=0)
    t2 = random_fail(fail_prob=1, inputs=[t1])
    t_final = random_fail(fail_prob=0, inputs=[t2])

    with pytest.raises(DependencyError):
        t_final.result()

    assert len(t_final.exception().dependent_exceptions_tids) == 1
    assert isinstance(t_final.exception().dependent_exceptions_tids[0][0], ManufacturedTestFailure)
