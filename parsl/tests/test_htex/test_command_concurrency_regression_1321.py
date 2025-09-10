import time
from threading import Event, Thread

import pytest

import parsl
from parsl.tests.configs.htex_local import fresh_config as local_config

N_THREADS = 50
DURATION_S = 10


@pytest.mark.local
def test_concurrency_blast():
    """Blast interchange command channel from many threads.
    """

    cc = parsl.dfk().executors['htex_local'].command_client

    threads = []

    ok_so_far = True

    for _ in range(N_THREADS):

        # This event will be set if the thread reaches the end of its body.
        event = Event()

        thread = Thread(target=blast, args=(cc, event))
        threads.append((thread, event))

    for thread, event in threads:
        thread.start()

    for thread, event in threads:
        thread.join()
        if not event.is_set():
            ok_so_far = False

    assert ok_so_far, "at least one thread did not exit normally"


def blast(cc, e):
    target_end = time.monotonic() + DURATION_S

    while time.monotonic() < target_end:
        cc.run("WORKERS")
        cc.run("MANGERs_PACKAGES")
        cc.run("CONNECTED_BLOCKS")
        cc.run("WORKER_BINDS")

    # If any of the preceeding cc.run calls raises an exception, the thread
    # will not set its successful completion event.
    e.set()
