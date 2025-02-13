"""Tests monitoring records app name under various decoration patterns.
"""

import logging
import os
import re
import time
from typing import Union

import pytest

import parsl
from parsl.config import Config
from parsl.data_provider.data_manager import default_staging
from parsl.data_provider.files import File
from parsl.data_provider.staging import Staging
from parsl.executors import HighThroughputExecutor
from parsl.monitoring import MonitoringHub
from parsl.providers import LocalProvider


def fresh_config(run_dir):
    return Config(
        run_dir=str(run_dir),
        executors=[
            HighThroughputExecutor(
                address="127.0.0.1",
                label="htex_Local",
                provider=LocalProvider(
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=1,
                )
            )
        ],
        strategy='simple',
        strategy_period=0.1,
        monitoring=MonitoringHub(
                        hub_address="localhost",
        )
    )


@parsl.python_app
def stdapp(stdout=None, stderr=None):
    pass


class ArbitraryPathLike(os.PathLike):
    def __init__(self, path: Union[str, bytes]) -> None:
        self.path = path

    def __fspath__(self) -> Union[str, bytes]:
        return self.path


class ArbitraryStaging(Staging):
    """This staging provider will not actually do any staging, but will
    accept arbitrary: scheme URLs. That's enough for this monitoring test
    which doesn't need any actual stage out action to happen.
    """
    def can_stage_out(self, file):
        return file.scheme == "arbitrary"


@pytest.mark.local
@pytest.mark.parametrize('stdx,expected_stdx',
                         [('hello.txt', 'hello.txt'),
                          (None, ''),
                          (('tuple.txt', 'w'), 'tuple.txt'),
                          (ArbitraryPathLike('pl.txt'), 'pl.txt'),
                          (ArbitraryPathLike(b'pl2.txt'), 'pl2.txt'),
                          ((ArbitraryPathLike('pl3.txt'), 'w'), 'pl3.txt'),
                          ((ArbitraryPathLike(b'pl4.txt'), 'w'), 'pl4.txt'),
                          (parsl.AUTO_LOGNAME,
                              lambda p:
                              isinstance(p, str) and
                              os.path.isabs(p) and
                              re.match("^.*/task_0000_stdapp\\.std...$", p)),
                          (File("arbitrary:abc123"), "arbitrary:abc123"),
                          (File("file:///tmp/pl5"), "file:///tmp/pl5"),
                          ])
@pytest.mark.parametrize('stream', ['stdout', 'stderr'])
def test_stdstream_to_monitoring(stdx, expected_stdx, stream, tmpd_cwd, caplog):
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

    c = fresh_config(tmpd_cwd)
    c.monitoring.logging_endpoint = f"sqlite:///{tmpd_cwd}/monitoring.db"
    c.executors[0].storage_access = default_staging + [ArbitraryStaging()]

    with parsl.load(c):
        kwargs = {stream: stdx}
        stdapp(**kwargs).result()

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
        result = connection.execute(sqlalchemy.text(f"SELECT task_{stream} FROM task"))
        (c, ) = result.first()

        if isinstance(expected_stdx, str):
            assert c == expected_stdx
        elif callable(expected_stdx):
            assert expected_stdx(c)
        else:
            raise RuntimeError("Bad expected_stdx value")

    for record in caplog.records:
        assert record.levelno < logging.ERROR
