import pytest

from parsl.config import Config
from parsl.errors import ConfigurationError


@pytest.mark.local
def test_invalid_values():
    """Test invalid usage_tracking values."""
    with pytest.raises(ConfigurationError):
        Config(usage_tracking=-1)

    with pytest.raises(ConfigurationError):
        Config(usage_tracking=4)
