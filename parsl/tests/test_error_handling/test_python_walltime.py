import pytest

import parsl
from parsl.app.errors import AppTimeout

@pytest.mark.local
@parsl.python_app
def my_app(walltime=1):
    import time
    time.sleep(1.2)
    return True

def test_python_walltime():
    f = my_app()
    with pytest.raises(AppTimeout):
        f.result()

with pytest.raises(TypeError):
    @pytest.mark.local
    @parsl.python_app(walltime=1)
    def my_app_2():
        import time
        time.sleep(1.2)
        return True
