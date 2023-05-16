import pytest
from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.errors import ConfigurationError

@pytest.mark.local
def test_no_executor_with_all_label():
    """Checks that the configuration system rejects configurations with
    an executor labelled 'all', which conflicts with the use of 'all' as
    a magic label to select all executors."""

    with pytest.raises(ConfigurationError):
        Config(executors=[ThreadPoolExecutor(label="all")])
