import pathlib

import pytest

import parsl


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
