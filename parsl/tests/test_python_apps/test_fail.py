import pytest

from parsl.app.app import python_app
from parsl.dataflow.errors import DependencyError


class ManufacturedTestFailure(Exception):
    pass


@python_app
def random_fail(fail_prob: float, inputs=()):
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


@pytest.mark.parametrize("fail_probs", ((1, 0), (0, 1)))
def test_fail_sequence(fail_probs):
    """Test failure in a sequence of dependencies

    App1 -> App2 ... -> AppN
    """

    t1_fail_prob, t2_fail_prob = fail_probs
    t1 = random_fail(fail_prob=t1_fail_prob)
    t2 = random_fail(fail_prob=t2_fail_prob, inputs=[t1])
    t_final = random_fail(fail_prob=0, inputs=[t2])

    with pytest.raises(DependencyError):
        t_final.result()


def test_deps(width=3):
    """Random failures in branches of Map -> Map -> reduce"""
    # App1   App2  ... AppN
    futs = [random_fail(fail_prob=0.4) for _ in range(width)]

    # App1   App2  ... AppN
    # |       |        |
    # V       V        V
    # App1   App2  ... AppN

    futs = [random_fail(fail_prob=0.8, inputs=[f]) for f in futs]

    # App1   App2  ... AppN
    #   |       |        |
    #   V       V        V
    # App1   App2  ... AppN
    #    \      |       /
    #     \     |      /
    # App_Final
    try:
        random_fail(fail_prob=0, inputs=futs).result()
    except DependencyError:
        pass
