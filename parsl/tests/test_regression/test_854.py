import multiprocessing
import random
import time

import pytest

from parsl.multiprocessing import MacSafeQueue


def consumer(in_q, out_q, delay=0):
    while True:
        x = in_q.get()
        time.sleep(delay)
        if x == 'STOP':
            out_q.put('STOPPED')
            break
        else:
            out_q.put(x)


@pytest.mark.local
def test_mac_safe_queue():
    """ Regression test for HTEX being broken on Mac OS: https://github.com/Parsl/parsl/issues/854
    This test doesn't test the fix on mac's however it tests a multiprocessing queue replacement
    that is safe to run on Mac OS.
    """
    task_q = MacSafeQueue()
    result_q = MacSafeQueue()

    p = multiprocessing.Process(target=consumer, args=(task_q, result_q,))
    p.start()
    for i in range(10):
        task_q.put(i)
        result_q.get()
    task_q.put('STOP')
    r = result_q.get()
    assert r == 'STOPPED', "Did not get stopped confirmation, got:{}".format(r)
    p.terminate()


@pytest.mark.local
def test_mac_safe_queue_size():
    """ Regression test for HTEX being broken on Mac OS: https://github.com/Parsl/parsl/issues/854
    This test doesn't test the fix on mac's however it tests a multiprocessing queue replacement
    that is safe to run on Mac OS.
    """
    task_q = MacSafeQueue()
    result_q = MacSafeQueue()

    x = random.randint(1, 100)

    [task_q.put(i) for i in range(x)]
    assert task_q.empty() is False, "Task queue should not be empty"
    assert task_q.qsize() == x, "Task queue should be {}; instead got {}".format(x, task_q.qsize())

    p = multiprocessing.Process(target=consumer, args=(task_q, result_q,))
    p.start()
    task_q.put('STOP')
    p.join()
    assert result_q.empty() is False, "Result queue should not be empty"
    qlen = result_q.qsize()
    assert qlen == x + 1, "Result queue should be {}; instead got {}".format(x + 1, qlen)
