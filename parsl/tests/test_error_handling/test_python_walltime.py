import parsl
from parsl.app.errors import AppTimeout


@parsl.python_app
def my_app(walltime=3):
    import time
    # the loop count must be substantially bigger than the walltime
    # but not infinite - so that the test eventually terminates
    # even if the walltime doesn't work.
    for n in range(0, 6):
        time.sleep(1)


def test_python_walltime():
    f = my_app()
    try:
        f.result()
        raise ValueError("Expected an exception, not a result")
    except AppTimeout:
        # AppTimeout is the correct result.
        # Any other exception should propagate upwards
        pass
