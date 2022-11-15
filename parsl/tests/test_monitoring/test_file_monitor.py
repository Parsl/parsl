import logging
import time

import pytest
from unittest import mock
import multiprocessing as mp
import dill
from threading import Thread
import re
import os
import glob

from parsl.monitoring.file_monitoring import validate_email, Emailer, proc_callback, \
                                             apply_async, run_dill_encoded, monitor, \
                                             FileMonitor

logger = logging.getLogger(__name__)


def test_validate_email():
    good_emails = ['test@abc.com', 'my.test@def.info', 'tester@abc.def.qrs.org']
    bad_emails = ['@abc.com', 'test.def.info', 'tester@edu']
    for g in good_emails:
        assert validate_email(g) is True

    for b in bad_emails:
        assert validate_email(b) is False

    assert validate_email(None) is False


def test_Emailer():
    assert Emailer.get_ssl() is False
    assert Emailer.is_valid() is False
    assert Emailer.get_task_id() is None

    Emailer.create("test@abc.com", 1234)

    assert Emailer.is_valid() is True
    assert Emailer.get_task_id() == 1234

    Emailer.invalidate()
    assert Emailer.is_valid() is False

    Emailer.set_ssl(True)
    assert Emailer.get_ssl() is True

    Emailer.create('test.def.info', 5678)
    assert Emailer.is_valid() is False

    with pytest.raises(TypeError):
        Emailer()


def test_proc_callback(caplog):
    class StrErr:
        def __str__(self):
            raise Exception()
    Emailer.invalidate()
    caplog.set_level(logging.INFO)
    test_msg= "Hello this is a test"
    proc_callback(test_msg)
    assert test_msg in caplog.text

    proc_callback(None)

    btest_msg = b"Another test message"
    proc_callback(btest_msg)
    assert btest_msg.decode() in caplog.text

    ltest_msg = [1,2,3,4,5]
    proc_callback(ltest_msg)
    assert str(ltest_msg) in caplog.text

    mmock = mock.Mock(spec=bytes)
    mmock.decode = mock.Mock(side_effect=Exception)
    proc_callback(mmock)
    assert "Could not decode" in caplog.text

    proc_callback(StrErr())
    assert "Could not turn" in caplog.text
    Emailer.set_ssl(False)
    testmock = mock.Mock(side_effect=Exception)
    Emailer.create('test@abc.cinfo', 1234)
    with mock.patch("smtplib.SMTP", testmock):
        with mock.patch("smtplib.SMTP_SSL", testmock):
            proc_callback("Hello")
            assert "Could not establish" in caplog.text
            assert Emailer.is_valid() is False

    Emailer.create('test@abc.cinfo', 1234)
    with mock.patch("smtplib.SMTP", testmock):
        with mock.patch("smtplib.SMTP_SSL", mock.MagicMock()):
            with mock.patch("email.message.EmailMessage", mock.MagicMock()):
                proc_callback("Hello")
                assert "Could not establish" in caplog.text
                assert Emailer.is_valid() is True

    with mock.patch("smtplib.SMTP", mock.MagicMock()):
        with mock.patch("email.message.EmailMessage", testmock):
            proc_callback("Last test message")
            assert "Could not send" in caplog.text
            assert Emailer.is_valid() is True
            assert "Last test" in caplog.text

    with mock.patch("smtplib.SMTP_SSL", testmock):
        proc_callback("Hello")
        assert "Could not establish" in caplog.text
        assert Emailer.is_valid() is False


def test_dill_functions():
    def add_two(a):
        return a + 2

    with mp.Pool(1) as pool:
        res = apply_async(pool, add_two, (6,))
        assert res.get() == 8

    payload = dill.dumps((add_two, (9,)))
    res = run_dill_encoded(payload)
    assert res == 11


def _test_png(files):
    for f in files:
        os.rename(f, f.replace(".png", ".done"))
    return "PNG files processed"


def _test_pdf(files):
    for f in files:
        with open(f, 'a') as fh:
            fh.write("processed\n")

    return "PDF files processed"


def _test_jpg(files):
    return "JPG files processed"


def _test_gif(files):
    return "GIF files processed"


def test_FileMonitor_init():
    with pytest.raises(Exception):
        fm = FileMonitor([_test_png])

    with pytest.raises(Exception):
        fm = FileMonitor([_test_png, _test_gif], filetype="png")

    fm = FileMonitor(_test_jpg, filetype=["jpg", "jpeg"])
    assert len(fm.patterns) == 2

    fm = FileMonitor([_test_png, _test_gif], filetype=["png", "gif"])
    assert len(fm.patterns) == 2

    fm = FileMonitor([_test_png, _test_pdf, _test_gif], pattern=r'results-(\S+)\.png', filetype=["*.pdf", ".gif"],
                     path="mypath")
    assert len(fm.patterns) == 3

    fm = FileMonitor([_test_png, _test_pdf, _test_gif], pattern=[r'results-(\S+)\.png', r'results-(\S+)\.pdf'],
                     filetype="gif", path="mypath")
    assert len(fm.patterns) == 3


@pytest.mark.issue363
def test_monitor():
    event1 = mp.Event()
    event2 = mp.Event()
    pngs = ["/tmp/results-parsl.png", "/tmp/results-par.png"]
    pdfs = ["/tmp/test1-pars.testme", "/tmp/test2-parsl.testme"]
    fnames1 = [pngs[0], pdfs[0]]
    fnames2 = [pdfs[1], pngs[1]]
    fnames3 = ["/tmp/test2-parsl.tesme", "/tmp/test2-parsl.png"]

    def _cleanup():
        for f in pngs:
            if os.path.exists(f):
                os.remove(f)
            if os.path.exists(f.replace(".png", ".done")):
                os.remove(f.replace(".png", ".done"))
        for f in pdfs + fnames3:
            if os.path.exists(f):
                os.remove(f)
    try:
        _cleanup()
        sleep_time = 3.0

        mthread = Thread(target=monitor, args=(1234, event1, event2, [(re.compile(r'results-(\S+)\.png'), True),
                                                                      ("*.testme", False)],
                                               [_test_png, _test_pdf], sleep_time, "/tmp/"))
        mthread.setDaemon(True)
        mthread.start()
        time.sleep(sleep_time + 1)
        for fn in fnames1:
            with open(fn, 'w') as fh:
                fh.write("\n")
        time.sleep(3 * sleep_time)
        for fn in fnames2 + fnames3:
            with open(fn, 'w') as fh:
                fh.write("\n")
        event1.set()
        assert event2.wait((sleep_time*2) + 1) is True
        for f in pngs:
            assert os.path.exists(f.replace(".png", ".done")) is True
            assert os.path.exists(f) is False
        for f in pdfs:
            assert os.path.exists(f)
            with open(f, 'r') as fh:
                rl = fh.readlines()
                assert "processed" in rl[1]
                assert len(rl) == 2
        for f in fnames3:
            assert os.path.exists(f)
    finally:
        _cleanup()


def test_wrapper():
    def _tfunc():
        return 3 + 4
    fm = FileMonitor([_test_png, _test_pdf, _test_gif], pattern=[r'results-(\S+)\.png', r'results-(\S+)\.pdf'],
                     filetype="gif", path="mypath")
    res = fm.file_monitor(_tfunc, 1234)
    assert res() == 7

