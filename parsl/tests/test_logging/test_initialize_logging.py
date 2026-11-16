import json
import logging
import pathlib

import pytest

import parsl
from parsl.logconfigs.file import FileLogging
from parsl.logconfigs.json import JSONHandler, JSONLogging
from parsl.logconfigs.noop import NoopLogging


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
def test_parsl_log_Noop_not_exists(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=NoopLogging())):
        assert not (pathlib.Path(parsl.dfk().run_dir) / "parsl.log").exists(), \
            "With NoopLogging, parsl.log should not exist in rundir after startup"


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


@pytest.mark.local
def test_parsl_log_JSONLogging(tmpd_cwd):
    with parsl.load(parsl.Config(run_dir=str(tmpd_cwd), initialize_logging=JSONLogging())):
        logger = logging.getLogger(__name__)
        msg1 = "Test without extra"
        msg2 = "Test with extra"

        logger.info(msg1)
        logger.info("Test with extra", extra={"parsl.somekey": "vv"})

        logfile = pathlib.Path(parsl.dfk().run_dir) / "parsl.jsonlog"
        assert logfile.exists(), \
            "With JSONLogging, parsl.jsonlog should exist in rundir after startup"

    # close down the DFK before attempt to read log file, so that there aren't any partial
    # writes.
    with open(logfile, "r") as f:
        lines = f.readlines()

        # we should parse every line as JSON, without Exceptions
        parsed = [json.loads(line) for line in lines]

        assert len([None for p in parsed if p['msg'] == msg1]) == 1, "msg1 should appear once"

        msgs2 = [p for p in parsed if p['msg'] == msg2]
        assert len(msgs2) == 1, "msg2 should appear once"
        assert msgs2[0]['parsl.somekey'] == "vv", "extra key should have been stored in record"


@pytest.mark.local
def test_JSONHandler_bad_record_does_not_raise(tmpd_cwd, capsys):
    """A record which cannot be formatted is reported via handleError, like the
    standard library handlers, rather than raised into the logging thread."""
    handler = JSONHandler(tmpd_cwd / "bad.jsonlog")
    logger = logging.getLogger("parsl.tests.json_bad_record")
    logger.propagate = False  # keep pytest's capturing handler, which re-raises, out of it
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    old_raise = logging.raiseExceptions
    logging.raiseExceptions = True  # make handleError report to stderr
    try:
        logger.debug("pending tasks: %d", "not a number")
        logger.debug("after the bad record")
    finally:
        logging.raiseExceptions = old_raise
        logger.removeHandler(handler)
        handler.close()

    assert "--- Logging error ---" in capsys.readouterr().err
    messages = [json.loads(line)["msg"] for line in (tmpd_cwd / "bad.jsonlog").read_text().splitlines()]
    assert messages == ["after the bad record"]
