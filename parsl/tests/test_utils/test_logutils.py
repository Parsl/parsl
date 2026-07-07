import io
import logging

import pytest

from parsl.log_utils import set_file_logger, set_stream_logger


@pytest.mark.local
def test_stream_close():
    """Tests that set_stream_logger callback detaches log handler.
    """

    logger = logging.getLogger("parsl")

    s1 = io.StringIO()
    close_callback_1 = set_stream_logger(stream=s1)
    logger.info("AAA")
    close_callback_1()

    s2 = io.StringIO()
    close_callback_2 = set_stream_logger(stream=s2)
    logger.info("BBB")
    close_callback_2()

    logger.info("CCC")

    assert "AAA" in s1.getvalue()
    assert "AAA" not in s2.getvalue()

    assert "BBB" not in s1.getvalue()
    assert "BBB" in s2.getvalue()

    assert "CCC" not in s1.getvalue()
    assert "CCC" not in s2.getvalue()


@pytest.mark.local
def test_file_close(tmpd_cwd):
    """Tests that set_file_Logger callback detaches log handler.
    """

    logger = logging.getLogger("parsl")

    f1 = str(tmpd_cwd / "log1")
    close_callback_1 = set_file_logger(filename=f1)
    logger.info("AAA")
    close_callback_1()

    f2 = str(tmpd_cwd / "log2")
    close_callback_2 = set_file_logger(filename=f2)
    logger.info("BBB")
    close_callback_2()

    logger.info("CCC")

    with open(f1, "r") as f:
        s1 = f.read()

    with open(f2, "r") as f:
        s2 = f.read()

    assert "AAA" in s1
    assert "AAA" not in s2

    assert "BBB" not in s1
    assert "BBB" in s2

    assert "CCC" not in s1
    assert "CCC" not in s2
