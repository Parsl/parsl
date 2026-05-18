import pathlib

import pytest

import parsl
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.logconfigs.file import FileLogging


@pytest.mark.local
def test_interchange_log_default(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd),
                                 executors=[HighThroughputExecutor(label="htex")])):
        assert (pathlib.Path(parsl.dfk().run_dir) / "htex" / "interchange.log").exists(), \
            "By default, interchange.log should exist in executor directory after startup"


@pytest.mark.local
def test_interchange_log_fileconfig(tmpd_cwd):
    # this arbitrary token should flow via the format_string parameter into the
    # resulting text-format log file, and the test will check it arrives there.
    token = "XXXXYYYYY"
    msg = f"{token} %(message)s"
    with parsl.load(parsl.Config(initialize_logging=FileLogging(format_string=msg),
                                 run_dir=str(tmpd_cwd),
                                 executors=[HighThroughputExecutor(label="htex")])):
        logfile = pathlib.Path(parsl.dfk().run_dir) / "htex" / "interchange.log"
        assert logfile.exists(), \
            "interchange.log should exist after startup"

    with open(logfile, "r") as f:
        ls = f.readlines()
        assert len(ls) >= 1, "There should be at least one log entry"
        assert ls[0].startswith(token), "Log entry should be formatted with token from format_string parameter"
