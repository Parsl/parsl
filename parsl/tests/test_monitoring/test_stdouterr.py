"""Tests monitoring records app name under various decoration patterns.
"""

import os
import parsl
import pytest
import re
import time

from typing import Union

from parsl.tests.configs.htex_local_alternate import fresh_config


@parsl.python_app
def stdapp(stdout=None, stderr=None):
    pass


class ArbitraryPathLike(os.PathLike):
    def __init__(self, path: Union[str, bytes]) -> None:
        self.path = path

    def __fspath__(self) -> Union[str, bytes]:
        return self.path


@pytest.mark.local
@pytest.mark.parametrize('stdx,expected_stdx',
                         [('hello.txt', 'hello.txt'),
                          (None, ''),
                          (('tuple.txt', 'w'), 'tuple.txt'),
                          (ArbitraryPathLike('pl.txt'), 'pl.txt'),
                          (ArbitraryPathLike(b'pl2.txt'), 'pl2.txt'),
                          ((ArbitraryPathLike('pl3.txt'), 'w'), 'pl3.txt'),
                          ((ArbitraryPathLike(b'pl4.txt'), 'w'), 'pl4.txt'),
                          (parsl.AUTO_LOGNAME, lambda p: isinstance(p, str) and os.path.isabs(p) and re.match("^.*/task_0000_stdapp\\.std...$", p))
                          ])
@pytest.mark.parametrize('stream', ['stdout', 'stderr'])
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

        if isinstance(expected_stdx, str):
            assert c == expected_stdx
        elif callable(expected_stdx):
            assert expected_stdx(c)
        else:
            raise RuntimeError("Bad expected_stdx value")
