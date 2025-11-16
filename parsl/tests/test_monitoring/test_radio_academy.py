import socket
import time

import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.academy import AcademyRadio
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_academy(tmpd_cwd):
    """Test academy radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    radio_config = AcademyRadio(atexit_timeout=0)

    # start receiver
    academy_receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                                    resource_msgs=resource_msgs)

    # check hash properties
    # assert len(radio_config.hmac_key) == 64, "With default hash, should expect 64 byte key"

    # make radio

    radio_sender = radio_config.create_sender()

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # verify it comes out of the receiver

    m = resource_msgs.get()

    assert m == message, "The sent message should appear in the queue"

    # shut down router

    academy_receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    # with pytest.raises(ValueError):
    #    academy_receiver.process.exitcode
