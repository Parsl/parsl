import pathlib

import pytest

import parsl
from parsl.logconfigs.file import FileLogging
from parsl.logconfigs.json import JSONLogging
from parsl.logconfigs.nolog import NoLogging


@pytest.mark.local
def test_parsl_log_exists(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd))):
        assert (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "By default, parsl.log should exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_not_exists_False_param(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=False)):
        assert not (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With initialize_logging=False, parsl.log should not exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_not_exists_NoLogging_config(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=NoLogging())):
        assert not (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With NoLogging, parsl.log should not exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_FileLogging(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=FileLogging())):
        assert (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With FileLogging, parsl.log should exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_JSONLogging(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=JSONLogging())):
        assert (pathlib.Path(parsl.dfk().run_dir) / "parsl.jsonlog").exists(), \
            "With JSONLogging, parsl.jsonlog should exist in rundir after startup"
