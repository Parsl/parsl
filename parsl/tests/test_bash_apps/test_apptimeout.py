import parsl
import pytest

from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import config
from parsl.app.errors import AppTimeout


@bash_app
def echo_to_file(inputs=(), outputs=(), walltime=0.01):
    return """echo "sleeping"; sleep 0.05"""


@pytest.mark.skip("walltime behaviour needs rearranging for Coroutine Parsl")
def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()
    with pytest.raises(AppTimeout):
        x.result()


@pytest.mark.skip("walltime behaviour needs rearranging for Coroutine Parsl")
def test_walltime_longer():
    """Test that an app that runs in less than walltime will succeed."""
    y = echo_to_file(walltime=0.2)
    y.result()
