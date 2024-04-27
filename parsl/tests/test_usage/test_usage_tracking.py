"""Test valid usage_tracking values."""

import pytest

import parsl
from parsl.config import Config


@pytest.mark.local
def test_config_load():
    """Test loading a config with usage tracking."""
    # Level 3
    with parsl.load(Config(usage_tracking=3)):
        pass
    parsl.clear()


@pytest.mark.local
def test_valid():
    """Test valid usage_tracking values."""
    # Level 0 (disabled)
    Config(usage_tracking=0)
    Config(usage_tracking=False)

    # Level 1
    Config(usage_tracking=1)

    # Level 2
    Config(usage_tracking=2)

    # Level 3
    Config(usage_tracking=3)
