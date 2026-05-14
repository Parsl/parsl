import pathlib

import pytest

import parsl
from parsl.logconfigs.file import FileLogging


@pytest.mark.local
def test_parsl_log_exists(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd))):
        assert (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "By default, parsl.log should exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_not_exists(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=False)):
        assert not (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With initialize_logging=False, parsl.log should not exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_FileLogging(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=FileLogging())):
        assert (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With FileLogging, parsl.log should exist in rundir after startup"


@pytest.mark.local
def test_parsl_log_FileLogging_format(tmpd_cwd):
    msg = "XXXYYY %(message)s"
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=FileLogging(format_string=msg))):
        logfile = pathlib.Path(parsl.dfk().run_dir) / "parsl.log"
        assert logfile.exists(), \
            "With FileLogging, parsl.log should exist in rundir after startup"
    with open(logfile, "r") as f:
        ls = f.readlines()
        assert len(ls) >= 1, "logfile should contain lines"
        assert ls[0].startswith("XXXYYY "), "logged lines should match format string"
