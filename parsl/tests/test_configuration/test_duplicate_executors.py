import parsl
import pytest

from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.errors import ConfigurationError


@pytest.mark.local
def test_duplicate_label_in_config():
    """Test that two executors with the same label cause a configuration error"""

    e1 = ThreadPoolExecutor(label="same")
    e2 = ThreadPoolExecutor(label="same")

    with pytest.raises(ConfigurationError):
        c = Config(executors = [e1, e2])
