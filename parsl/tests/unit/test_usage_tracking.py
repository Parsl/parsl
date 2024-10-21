"""Test usage_tracking values."""

import pytest

import parsl
from parsl.config import Config
from parsl.errors import ConfigurationError


@pytest.mark.local
def test_config_load():
    """Test loading a config with usage tracking."""
    with parsl.load(Config(usage_tracking=3)):
        pass
    parsl.clear()


@pytest.mark.local
@pytest.mark.parametrize("level", (0, 1, 2, 3, False, True))
def test_valid(level):
    """Test valid usage_tracking values."""
    Config(usage_tracking=level)
    assert Config(usage_tracking=level).usage_tracking == level


@pytest.mark.local
@pytest.mark.parametrize("level", (12, 1000, -1))
def test_invalid_values(level):
    """Test invalid usage_tracking values."""
    with pytest.raises(ConfigurationError):
        Config(usage_tracking=level)


@pytest.mark.local
@pytest.mark.parametrize("level", ("abcd", None, bytes(1), 1.0, 1j, object()))
def test_invalid_types(level):
    """Test invalid usage_tracking types."""
    with pytest.raises(Exception) as ex:
        Config(usage_tracking=level)

    # with typeguard 4.x this is TypeCheckError,
    # with typeguard 2.x this is TypeError
    # we can't instantiate TypeCheckError if we're in typeguard 2.x environment
    # because it does not exist... so check name using strings.
    assert ex.type.__name__ in ["TypeCheckError", "TypeError"]


@pytest.mark.local
def test_valid_project_name():
    """Test valid project_name."""
    assert (
        Config(
            usage_tracking=3,
            project_name="unit-test",
        ).project_name == "unit-test"
    )


@pytest.mark.local
@pytest.mark.parametrize("name", (1, 1.0, True, object()))
def test_invalid_project_name(name):
    """Test invalid project_name."""
    with pytest.raises(Exception) as ex:
        Config(usage_tracking=3, project_name=name)

    assert ex.type.__name__ in ["TypeCheckError", "TypeError"]
