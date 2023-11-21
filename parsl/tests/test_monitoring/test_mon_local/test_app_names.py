import logging
import os
import parsl
import pytest
import time

logger = logging.getLogger(__name__)


@parsl.python_app
def regular_decorated_app():
    return 5


def for_decoration_later():
    return 77


@pytest.mark.local
def test_regular_decorated_app():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from parsl.tests.configs.local_threads_monitoring import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("Generating fresh config")
    c = fresh_config()
    logger.info("Loading parsl")
    parsl.load(c)

    logger.info("invoking and waiting for result")
    assert regular_decorated_app().result() == 5

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        result = connection.execute("SELECT COUNT(*) FROM workflow")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM task")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM try")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT task_func_name FROM task")
        (c, ) = result.first()
        assert c == "regular_decorated_app"

    logger.info("all done")


@pytest.mark.local
def test_for_decoration_later():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from parsl.tests.configs.local_threads_monitoring import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("Generating fresh config")
    c = fresh_config()
    logger.info("Loading parsl")
    parsl.load(c)

    a = parsl.python_app(for_decoration_later)

    logger.info("invoking and waiting for result")
    assert a().result() == 77

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        result = connection.execute("SELECT COUNT(*) FROM workflow")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM task")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM try")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT task_func_name FROM task")
        (c, ) = result.first()
        assert c == "for_decoration_later"

    logger.info("all done")


@pytest.mark.local
def test_decorated_closure():
    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy
    from parsl.tests.configs.local_threads_monitoring import fresh_config

    if os.path.exists("runinfo/monitoring.db"):
        logger.info("Monitoring database already exists - deleting")
        os.remove("runinfo/monitoring.db")

    logger.info("Generating fresh config")
    c = fresh_config()
    logger.info("Loading parsl")
    parsl.load(c)

    @parsl.python_app
    def inner_function():
        return 53

    logger.info("invoking and waiting for result")
    assert inner_function().result() == 53

    logger.info("cleaning up parsl")
    parsl.dfk().cleanup()
    parsl.clear()

    # at this point, we should find one row in the monitoring database.

    logger.info("checking database content")
    engine = sqlalchemy.create_engine("sqlite:///runinfo/monitoring.db")
    with engine.begin() as connection:

        result = connection.execute("SELECT COUNT(*) FROM workflow")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM task")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT COUNT(*) FROM try")
        (c, ) = result.first()
        assert c == 1

        result = connection.execute("SELECT task_func_name FROM task")
        (c, ) = result.first()
        assert c == "inner_function"

    logger.info("all done")
