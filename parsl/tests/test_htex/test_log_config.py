import pathlib

import pytest

import parsl
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.logconfigs.file import FileLogging


@parsl.python_app
def noop_app():
    pass


@pytest.mark.local
def test_interchange_log_default(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd),
                                 executors=[HighThroughputExecutor(label="htex")])):
        assert (pathlib.Path(parsl.dfk().run_dir) / "htex" / "interchange.log").exists(), \
            "By default, interchange.log should exist in executor directory after startup"


@pytest.mark.local
def test_log_fileconfig(tmpd_cwd):
    # this arbitrary token should flow via the format_string parameter into the
    # resulting text-format log file, and the test will check it arrives there.
    token = "XXXXYYYYY"
    msg = f"{token} %(message)s"
    with parsl.load(parsl.Config(initialize_logging=FileLogging(format_string=msg),
                                 run_dir=str(tmpd_cwd),
                                 executors=[HighThroughputExecutor(label="htex")])):
        # ensure that enough has started to run a task - so that includes everything
        # all the way to a worker, on the task execution path
        noop_app().result()

        interchange_logfile = pathlib.Path(parsl.dfk().run_dir) / "htex" / "interchange.log"

        # assume in this configuration that there is only one subdirectory of block-0,
        # and that it is the directory for a started worker pool.
        pool_dir = next((pathlib.Path(parsl.dfk().run_dir) / "htex" / "block-0").iterdir())
        manager_logfile = pool_dir / "manager.log"
        worker_logfile = pool_dir / "worker_0.log"

        assert interchange_logfile.exists(), \
            "interchange.log should exist after startup"
        assert manager_logfile.exists(), \
            "a manager log file should exist after startup"
        assert worker_logfile.exists(), \
            "a worker log file should exist after startup"

    def check_file(logfile: pathlib.Path):
        with open(logfile, "r") as f:
            ls = f.readlines()
            assert len(ls) >= 1, f"There should be at least one log entry in {logfile}"
            assert ls[0].startswith(token), f"Log entry in {logfile} should be formatted with token from format_string parameter"

    check_file(interchange_logfile)
    check_file(manager_logfile)
    check_file(worker_logfile)
