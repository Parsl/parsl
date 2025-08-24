import pytest

from parsl.monitoring.radios.zmq import ZMQRadioSender
from parsl.monitoring.radios.zmq_router import start_zmq_receiver
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_send_recv_message(tmpd_cwd, try_assert):
    q = SpawnQueue()
    loopback = "127.0.0.1"
    r = start_zmq_receiver(monitoring_messages=q,
                           loopback_address=loopback,
                           port_range=(49152, 65535),
                           logdir=str(tmpd_cwd),
                           worker_debug=False)

    s = ZMQRadioSender(loopback, r.port)

    test_msg = ("test", {})
    s.send(test_msg)

    assert q.get() == test_msg

    assert r.process.is_alive()
    r.exit_event.set()
    try_assert(lambda: not r.process.is_alive())
