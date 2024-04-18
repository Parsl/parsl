"""Tests monitoring records app name under various decoration patterns.
"""

import os
import parsl
import pytest
import time

from parsl.tests.configs.htex_local_alternate import fresh_config


@parsl.python_app
def stdapp(stdout=None, stderr=None):
    pass


@pytest.mark.local
@pytest.mark.parametrize("stdx,expected_stdx",
                         [("hello.txt", "hello.txt"),
                          (None, ""),
                          (("tuple.txt", "w"), "tuple.txt")
                          ])  # TODO: how to test the result of PARSL_AUTO here? is it by using a predicate not a string?
@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_stdstream_to_monitoring(stdx, expected_stdx, stream, tmpd_cwd):
    """This tests that various forms of stdout/err specification are
       represented in monitoring correctly. The stderr and stdout codepaths
       are generally duplicated, rather than factorised, and so this test
       runs the same tests on both stdout and stderr.
    """

    # this is imported here rather than at module level because
    # it isn't available in a plain parsl install, so this module
    # would otherwise fail to import and break even a basic test
    # run.
    import sqlalchemy

    c = fresh_config()
    c.run_dir = tmpd_cwd
    c.monitoring.logging_endpoint = f"sqlite:///{tmpd_cwd}/monitoring.db"
    with parsl.load(c):
        kwargs = {stream: stdx}
        stdapp(**kwargs).result()

    parsl.clear()

    engine = sqlalchemy.create_engine(c.monitoring.logging_endpoint)
    with engine.begin() as connection:

        def count_rows(table: str):
            result = connection.execute(f"SELECT COUNT(*) FROM {table}")
            (c, ) = result.first()
            return c

        # one workflow...
        assert count_rows("workflow") == 1

        # ... with one task ...
        assert count_rows("task") == 1

        # ... that was tried once ...
        assert count_rows("try") == 1

        # ... and has the expected name.
        result = connection.execute(f"SELECT task_{stream} FROM task")
        (c, ) = result.first()
        assert c == expected_stdx
