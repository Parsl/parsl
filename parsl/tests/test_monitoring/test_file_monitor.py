import logging
import time

import pytest
from unittest import mock
import multiprocessing as mp
import dill
from threading import Thread
import re
import os

from parsl.monitoring.file_monitoring import proc_callback, \
                                             apply_async, run_dill_encoded, monitor, \
                                             FileMonitor

logger = logging.getLogger(__name__)


def test_proc_callback(caplog):
    class StrErr:
        def __str__(self):
            raise Exception()
    caplog.set_level(logging.INFO)
    test_msg = "Hello this is a test"
    proc_callback(test_msg)
    assert test_msg in caplog.text

    proc_callback(None)

    btest_msg = b"Another test message"
    proc_callback(btest_msg)
    assert btest_msg.decode() in caplog.text

    ltest_msg = [1, 2, 3, 4, 5]
    proc_callback(ltest_msg)
    assert str(ltest_msg) in caplog.text

    mmock = mock.Mock(spec=bytes)
    mmock.decode = mock.Mock(side_effect=Exception)
    proc_callback(mmock)
    assert "Could not decode" in caplog.text

    proc_callback(StrErr())
    assert "Could not turn" in caplog.text


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
    fm = FileMonitor([(_test_jpg, "jpg")])
    assert len(fm.patterns) == 1

    fm = FileMonitor([(_test_png, "png"), (_test_gif, "gif")])
    assert len(fm.patterns) == 2

    fm = FileMonitor([(_test_png, re.compile(r'results-(\S+)\.png')), (_test_pdf, "*.pdf"), (_test_gif, ".gif")],
                     path="mypath")
    assert len(fm.patterns) == 3


@pytest.mark.issue363
def test_monitor(caplog):
    caplog.set_level(logging.INFO)
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

        mthread = Thread(target=monitor, args=(1234, event1, event2, [(_test_png, re.compile(r'results-(\S+)\.png')),
                                                                      (_test_pdf, "*.testme")],
                                               sleep_time, "/tmp/"))
        mthread.daemon = True
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
        assert event2.wait((sleep_time * 2) + 1) is True
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
    fm = FileMonitor([(_test_png, re.compile(r'results-(\S+)\.png')), (_test_gif, "gif"), (_test_pdf, re.compile(r'results-(\S+)\.pdf'))],
                     path="mypath")
    res = fm.file_monitor(_tfunc, 1234)
    assert res() == 7
