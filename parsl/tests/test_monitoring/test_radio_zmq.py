import pytest

import parsl.curvezmq as curvezmq

from parsl.monitoring.radios.zmq import ZMQRadioSender
from parsl.monitoring.radios.zmq_router import start_zmq_receiver
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (False, True))
def test_send_recv_message(tmpd_cwd, try_assert, encrypted):

    if encrypted:
        cert_dir = curvezmq.create_certificates(tmpd_cwd)
    else:
        cert_dir = None

    q = SpawnQueue()
    loopback = "127.0.0.1"
    r = start_zmq_receiver(monitoring_messages=q,
                           loopback_address=loopback,
                           port_range=(49152, 65535),
                           logdir=str(tmpd_cwd),
                           worker_debug=False,
                           cert_dir=cert_dir)

    s = ZMQRadioSender(loopback, r.port, cert_dir=cert_dir)

    test_msg = ("test", {})
    s.send(test_msg)

    assert q.get() == test_msg

    assert r.process.is_alive()
    r.exit_event.set()
    try_assert(lambda: not r.process.is_alive())
