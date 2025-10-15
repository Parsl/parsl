import parsl
from parsl import python_app
from parsl.dataflow.errors import DependencyError
from parsl.dataflow.states import States


@python_app
def fails():
    raise ValueError("Deliberate failure")


@python_app
def depends(parent):
    return 1


def test_depfail_once():
    """Test the simplest dependency failure case"""
    start_dep_fail_count = parsl.dfk().task_state_counts[States.dep_fail]
    f1 = fails()
    f2 = depends(f1)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)

    # check that the task ID of the failing task is mentioned
    # in the DependencyError message
    assert ("task " + str(f1.task_record['id'])) in str(f2.exception())

    assert parsl.dfk().task_state_counts[States.dep_fail] == start_dep_fail_count + 1


def test_depfail_chain():
    """Test that dependency failures chain"""
    start_dep_fail_count = parsl.dfk().task_state_counts[States.dep_fail]
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f2)
    f4 = depends(f3)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)
    assert isinstance(f3.exception(), DependencyError)
    assert isinstance(f4.exception(), DependencyError)

    assert parsl.dfk().task_state_counts[States.dep_fail] == start_dep_fail_count + 3


def test_depfail_branches():
    """Test that dependency failures propagate in the
    presence of multiple downstream tasks."""
    start_dep_fail_count = parsl.dfk().task_state_counts[States.dep_fail]
    f1 = fails()
    f2 = depends(f1)
    f3 = depends(f1)

    assert isinstance(f1.exception(), Exception)
    assert not isinstance(f1.exception(), DependencyError)
    assert isinstance(f2.exception(), DependencyError)
    assert isinstance(f3.exception(), DependencyError)

    assert parsl.dfk().task_state_counts[States.dep_fail] == start_dep_fail_count + 2
