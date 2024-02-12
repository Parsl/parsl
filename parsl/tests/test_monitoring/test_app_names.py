"""Tests monitoring records app name under various decoration patterns.
"""

import logging
import os
import parsl
import pytest
import time

from parsl.tests.configs.htex_local_alternate import fresh_config

logger = logging.getLogger(__name__)


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
def test_app_name(get_app, expected_name, expected_result):

    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("Generating fresh config")
    c = fresh_config()
    logger.info("Loading parsl")
    parsl.load(c)

    logger.info("invoking and waiting for result")
    app = get_app()
    assert app().result() == expected_result

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        # one workflow...
        result = connection.execute("SELECT COUNT(*) FROM workflow")
        (c, ) = result.first()
        assert c == 1

        # ... with one task ...
        result = connection.execute("SELECT COUNT(*) FROM task")
        (c, ) = result.first()
        assert c == 1

        # ... that was tried once ...
        result = connection.execute("SELECT COUNT(*) FROM try")
        (c, ) = result.first()
        assert c == 1

        # ... and has the expected name.
        result = connection.execute("SELECT task_func_name FROM task")
        (c, ) = result.first()
        assert c == expected_name

    logger.info("all done")
