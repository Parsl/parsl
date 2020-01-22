import pytest

import parsl
from parsl.app.errors import AppTimeout


@parsl.python_app
def my_app(walltime=1):
    import time
    time.sleep(1.2)
    return True


def test_python_walltime():
    f = my_app()
    with pytest.raises(AppTimeout):
        f.result()


def test_python_longer_walltime_at_invocation():
    f = my_app(walltime=6)
    f.result()


def test_python_bad_decorator_args():

    with pytest.raises(TypeError):
        @pytest.mark.local
        @parsl.python_app(walltime=1)
        def my_app_2():
            import time
            time.sleep(1.2)
            return True
