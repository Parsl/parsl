import pytest

import parsl
from parsl.app.errors import AppTimeout


@parsl.python_app
def my_app(t=0.0, walltime=0.1):
    import time
    end = time.monotonic() + t
    while time.monotonic() < end:
        time.sleep(0.01)  # work with timeout(); allow access to thread
    return 7


def test_python_walltime():
    f = my_app(2)
    with pytest.raises(AppTimeout):
        f.result()


def test_python_longer_walltime_at_invocation():
    f = my_app(0.01, walltime=6)
    assert f.result() == 7


def test_python_walltime_wrapped_names():
    f = my_app(0.01, walltime=6)
    assert f.result() == 7
    assert f.task_def['func'].__name__ == "my_app"
    assert f.task_def['func_name'] == "my_app"


def test_python_bad_decorator_args():

    with pytest.raises(TypeError):
        @pytest.mark.local
        @parsl.python_app(walltime=1)
        def my_app_2():
            import time
            time.sleep(1.2)
            return True
