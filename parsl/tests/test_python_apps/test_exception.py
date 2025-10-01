import pytest

from parsl.app.app import python_app


class CustomException(Exception):
    pass


@python_app
def custom_exception():
    from parsl.tests.test_python_apps.test_exception import CustomException
    raise CustomException('foobar')


def test_custom_exception():
    x = custom_exception()
    with pytest.raises(CustomException):
        x.result()
