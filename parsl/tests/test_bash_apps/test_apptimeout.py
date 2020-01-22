import parsl
import pytest

from parsl.app.app import bash_app
from parsl.tests.configs.local_threads import config
from parsl.app.errors import AppTimeout


@bash_app
def echo_to_file(inputs=[], outputs=[], stderr='std.err', stdout='std.out', walltime=0.5):
    return """echo "sleeping";
    sleep 1 """


def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()
    with pytest.raises(AppTimeout):
        x.result()


def test_walltime_longer():
    """Test that an app that runs in less than walltime will succeed."""
    y = echo_to_file(walltime=2)
    y.result()


if __name__ == "__main__":
    parsl.clear()
    parsl.load(config)
    test_walltime()
