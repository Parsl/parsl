# make several threads of blasting concurrency at the interchange

import logging
import time
from threading import Event, Thread

import pytest

import parsl
from parsl.tests.configs.htex_local import fresh_config as local_config

logger = logging.getLogger(__name__)

N_THREADS = 50
DURATION_S = 10


@pytest.mark.local
def test_concurrency_blast():
    # create some threads
    # have them all blast the interchange command channel for stuff for 1 minute

    cc = parsl.dfk().executors['htex_local'].command_client

    threads = []

    ok_so_far = True

    for _ in range(N_THREADS):
        event = Event()
        thread = Thread(target=blast, args=(cc, event))
        threads.append((thread, event))

    for thread, event in threads:
        thread.start()

    for thread, event in threads:
        thread.join()
        if not event.is_set():
            logger.error("thread should have exited normally, but did not")
            ok_so_far = False

    assert ok_so_far, "at least one thread did not exit normally"


def blast(cc, e):
    target_end = time.time() + DURATION_S

    while time.time() < target_end:
        cc.run("WORKERS")
        cc.run("MANGERs_PACKAGES")
        cc.run("CONNECTED_BLOCKS")
        cc.run("WORKER_BINDS")

    e.set()
