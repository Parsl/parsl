import socket
import time

import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.udp import UDPRadio
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_udp(tmpd_cwd):
    """Test UDP radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    radio_config = UDPRadio(address="localhost", atexit_timeout=0)

    # start receiver
    udp_receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                                resource_msgs=resource_msgs)

    # check hash properties

    assert len(radio_config.hmac_key) == 64, "With default hash, should expect 64 byte key"

    # make radio

    radio_sender = radio_config.create_sender()

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # verify it comes out of the receiver

    m = resource_msgs.get()

    assert m == message, "The sent message should appear in the queue"

    # shut down router

    udp_receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        udp_receiver.process.exitcode


@pytest.mark.local
def test_bad_hmac(tmpd_cwd, caplog):
    """Test when HMAC does not match.
    """

    resource_msgs = SpawnQueue()

    radio_config = UDPRadio(address="localhost", atexit_timeout=0)

    # start receiver
    udp_receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                                resource_msgs=resource_msgs)

    # check the hmac is configured in the right place,
    # then change it to something different (by prepending a new byte)
    assert radio_config.hmac_key is not None
    radio_config.hmac_key += b'x'

    # make radio, after changing the HMAC key

    radio_sender = radio_config.create_sender()

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # We should expect no message from the UDP side. That's hard to
    # state in this scenario because UDP doesn't have any delivery
    # guarantees for the test-failing case.
    # So sleep a while to allow that test to misdeliver and fail.
    time.sleep(1)

    assert resource_msgs.empty(), "receiving queue should be empty"
    assert udp_receiver.process.is_alive(), "UDP router process should still be alive"

    with open(f"{tmpd_cwd}/monitoring_udp_router.log", "r") as logfile:
        assert "ERROR" in logfile.read(), "Router log file should contain an error"

    # shut down router

    udp_receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        udp_receiver.process.exitcode


@pytest.mark.local
def test_wrong_digest(tmpd_cwd, caplog):
    """Test when HMAC does not match.
    """

    resource_msgs = SpawnQueue()

    radio_config = UDPRadio(address="localhost", atexit_timeout=0)

    # start receiver
    udp_receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                                resource_msgs=resource_msgs)

    # check the hmac is configured in the right place,
    # then change it to a different digest. The choice of different
    # digest is arbitrary.
    assert radio_config.hmac_digest is not None
    radio_config.hmac_digest = "sha3_224"

    # make radio, after changing the HMAC digest

    radio_sender = radio_config.create_sender()

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # We should expect no message from the UDP side. That's hard to
    # state in this scenario because UDP doesn't have any delivery
    # guarantees for the test-failing case.
    # So sleep a while to allow that test to misdeliver and fail.
    time.sleep(1)

    assert resource_msgs.empty(), "receiving queue should be empty"
    assert udp_receiver.process.is_alive(), "UDP router process should still be alive"

    with open(f"{tmpd_cwd}/monitoring_udp_router.log", "r") as logfile:
        assert "ERROR" in logfile.read(), "Router log file should contain an error"

    # shut down router

    udp_receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        udp_receiver.process.exitcode


@pytest.mark.local
def test_short_message(tmpd_cwd, caplog):
    """Test when UDP message is so short it can't even be parsed into
    HMAC + the rest.
    """

    resource_msgs = SpawnQueue()

    radio_config = UDPRadio(address="localhost", atexit_timeout=0)

    # start receiver
    udp_receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                                resource_msgs=resource_msgs)

    # now send a bad UDP message, rather than using the sender mechanism.

    sock = socket.socket(socket.AF_INET,
                         socket.SOCK_DGRAM,
                         socket.IPPROTO_UDP)

    sock.sendto(b'', (radio_config.address, radio_config.port))
    sock.close()

    # We should expect no message from the UDP side. That's hard to
    # state in this scenario because UDP doesn't have any delivery
    # guarantees for the test-failing case.
    # So sleep a while to allow that test to misdeliver and fail.
    time.sleep(1)

    assert resource_msgs.empty(), "receiving queue should be empty"
    assert udp_receiver.process.is_alive(), "UDP router process should still be alive"

    with open(f"{tmpd_cwd}/monitoring_udp_router.log", "r") as logfile:
        assert "ERROR" in logfile.read(), "Router log file should contain an error"

    # shut down router

    udp_receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        udp_receiver.process.exitcode
