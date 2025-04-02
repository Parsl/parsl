import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.udp import UDPRadioSender
from parsl.monitoring.radios.udp_router import start_udp_receiver
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_udp(tmpd_cwd):
    """Test UDP radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    # start receiver
    udp_receiver = start_udp_receiver(debug=True,
                                      logdir=str(tmpd_cwd),
                                      monitoring_messages=resource_msgs,
                                      port=None
                                      )

    # make radio

    # this comes from monitoring.py:
    url = "udp://{}:{}".format("localhost", udp_receiver.port)

    radio_sender = UDPRadioSender(url)

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # verify it comes out of the receiver

    m = resource_msgs.get()

    assert m == message, "The sent message should appear in the queue"

    # shut down router

    udp_receiver.close()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        udp_receiver.process.exitcode
