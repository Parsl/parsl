import parsl
import pytest
from parsl.tests.configs.local_threads import fresh_config


@pytest.mark.local
def test_summary(caplog):

    parsl.load(fresh_config())
    parsl.dfk().cleanup()
    parsl.clear()

    assert "Summary of tasks in DFK:" in caplog.text

