import pytest
import parsl
from parsl import ThreadPoolExecutor
from parsl.config import Config
from parsl.errors import ConfigurationError


@pytest.mark.local
def test_no_executor_with_all_label() -> None:
    """Checks that the configuration system rejects configurations with
    an executor labelled 'all', which conflicts with the use of 'all' as
    a magic label to select all executors."""

    with pytest.raises(ConfigurationError):
        Config(executors=[ThreadPoolExecutor(label="all")])


@pytest.mark.local
def test_no_add_executor_with_all_label() -> None:
    """Checks that the configuration system rejects configurations with
    an executor labelled 'all', which conflicts with the use of 'all' as
    a magic label to select all executors."""

    c = Config(executors=[ThreadPoolExecutor(label="executor1")])
    dfk = parsl.load(c)

    # check we can add an executor successfully
    dfk.add_executors([ThreadPoolExecutor(label="executor2")])

    # check we cannot add an executor called 'all'
    with pytest.raises(ConfigurationError):
        dfk.add_executors([ThreadPoolExecutor(label="all")])

    dfk.cleanup()
    parsl.clear()
