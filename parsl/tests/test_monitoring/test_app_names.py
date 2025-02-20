"""Tests monitoring records app name under various decoration patterns.
"""

import os
import time

import pytest

import parsl
from parsl.tests.configs.htex_local_alternate import fresh_config


@parsl.python_app
def regular_decorated_app():
    return 5


@pytest.mark.local
def get_regular_decorated_app():
    return regular_decorated_app


def for_decoration_later():
    return 77


def get_for_decoration_later():
    return parsl.python_app(for_decoration_later)


def get_decorated_closure():

    r = 53

    @parsl.python_app
    def decorated_closure():
        return r

    return decorated_closure


@pytest.mark.local
@pytest.mark.parametrize("get_app,expected_name,expected_result",
                         [(get_regular_decorated_app, "regular_decorated_app", 5),
                          (get_for_decoration_later, "for_decoration_later", 77),
                          (get_decorated_closure, "decorated_closure", 53)
                          ])
def test_app_name(get_app, expected_name, expected_result, tmpd_cwd):

    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy

    c = fresh_config()
    c.run_dir = tmpd_cwd
    c.monitoring.logging_endpoint = f"sqlite:///{tmpd_cwd}/monitoring.db"
    parsl.load(c)

    app = get_app()
    assert app().result() == expected_result

    parsl.dfk().cleanup()

    engine = sqlalchemy.create_engine(c.monitoring.logging_endpoint)
    with engine.begin() as connection:

        def count_rows(table: str):
            result = connection.execute(sqlalchemy.text(f"SELECT COUNT(*) FROM {table}"))
            (c, ) = result.first()
            return c

        # one workflow...
        assert count_rows("workflow") == 1

        # ... with one task ...
        assert count_rows("task") == 1

        # ... that was tried once ...
        assert count_rows("try") == 1

        # ... and has the expected name.
        result = connection.execute(sqlalchemy.text("SELECT task_func_name FROM task"))
        (c, ) = result.first()
        assert c == expected_name
