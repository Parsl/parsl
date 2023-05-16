import parsl
import pytest

from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.errors import ConfigurationError


@pytest.mark.local
def test_duplicate_label_in_config():
    """Test that two executors with the same label cause a configuration error,
    when configured together."""

    e1 = ThreadPoolExecutor(label="same")
    e2 = ThreadPoolExecutor(label="same")

    c = Config(executors=[e1, e2])
    with pytest.raises(ConfigurationError):
        parsl.load(c)

    # TODO: this leaves a partially constructed DFK which will have already
    # started threads in __init__ but will not shut them down when the
    # configuration exception is raised.


@pytest.mark.local
def test_duplicate_label_in_config_add_executor():
    """Test that two executors with the same label cause a configuration error,
    when an executor added with add_executor has the same label as an already
    configured executor.
    """

    e1 = ThreadPoolExecutor(label="same")
    e2 = ThreadPoolExecutor(label="same")

    c = Config(executors=[e1])

    dfk = parsl.load(c)

    with pytest.raises(ConfigurationError):
        dfk.add_executors([e2])

    dfk.cleanup()
    parsl.clear()
