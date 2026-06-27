import pathlib

import pytest

import parsl
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from parsl.logconfigs.json import JSONLogging
from parsl.monitoring import MonitoringHub


def config(*, run_dir):
    c = Config(executors=[ThreadPoolExecutor()],
               monitoring=MonitoringHub(),
               run_dir=str(run_dir))
    return c


def config_log(*, run_dir, log_config):
    c = Config(executors=[ThreadPoolExecutor()],
               monitoring=MonitoringHub(),
               run_dir=str(run_dir),
               initialize_logging=log_config)
    return c


@pytest.mark.local
def test_log_default_config(tmpd_cwd, try_assert):
    with parsl.load(config(run_dir=tmpd_cwd)):
        try_assert(lambda: (pathlib.Path(parsl.dfk().run_dir) / "database_manager.log").exists())


@pytest.mark.local
def test_log_json_config(tmpd_cwd, try_assert):
    with parsl.load(config_log(run_dir=tmpd_cwd, log_config=JSONLogging())):
        try_assert(lambda: (pathlib.Path(parsl.dfk().run_dir) / "database_manager.jsonlog").exists())
