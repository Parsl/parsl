import parsl
import pytest

from parsl.tests.configs.htex_local import fresh_config as local_config


@parsl.python_app(executors=['htex_local'])
def app_executor_list():
    return 7


@pytest.mark.local
def test_executor_list() -> None:
    assert app_executor_list().result() == 7


@parsl.python_app(executors='htex_local')
def app_executor_str():
    return 8


@pytest.mark.local
def test_executor_str() -> None:
    assert app_executor_str().result() == 8


@parsl.python_app(executors='XXXX_BAD_EXECUTOR')
def app_executor_invalid():
    return 9


@pytest.mark.local
def test_executor_invalid() -> None:
    with pytest.raises(ValueError):
        app_executor_invalid().result()


@parsl.python_app(executors='all')
def app_executor_all():
    return 10


@pytest.mark.local
def test_executor_all() -> None:
    assert app_executor_all().result() == 10
