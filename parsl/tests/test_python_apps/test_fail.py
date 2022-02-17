from parsl.app.app import python_app
from parsl.dataflow.error import DependencyError


@python_app
def fail_app():
    raise RuntimeError("fail_app always fails")


@python_app
def pass_app(inputs=[]):
    return None


def test_no_deps(numtasks=2):
    """Test basic error handling, with no dependent failures
    """

    fus = []
    for i in range(0, numtasks):

        fu = fail_app()
        fus.extend([fu])

    for fu in fus:
        assert isinstance(fu.exception(), Exception), "All futures must be exceptions"


def test_fail_sequence(numtasks=10):
    """Test failure in a sequence of dependencies

    App1 -> App2 ... -> AppN
    """

    f = fail_app()
    for i in range(0, numtasks):
        f = pass_app(inputs=[f])

    assert(isinstance(f.exception(), DependencyError)), "Final task did not fail with a dependency exception"
