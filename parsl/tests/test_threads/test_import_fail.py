"""Testing python apps
"""
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def platform_name():
    return platform.platform()


def test_NameError(n=2):
    """Catch NameError for missing name
    """

    p = platform_name()

    try:
        p.result()
    except NameError:
        print("Caught NameError")
    else:
        assert False, "Raise the wrong Error"


@App('python', dfk)
def bad_import():
    import non_existent
    return non_existent.foo()


def test_ImportError(n=2):
    """Catch ImportError for missing name
    """

    p = bad_import()

    try:
        p.result()
    except ImportError:
        print("Caught ImportError")
    else:
        assert False, "Raise the wrong Error"
