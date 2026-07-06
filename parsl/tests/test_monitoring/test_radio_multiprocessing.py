import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadio
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_radio(tmpd_cwd):
    """Test filesystem radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    radio_config = MultiprocessingQueueRadio()

    # start receiver
    receiver = radio_config.create_receiver(run_dir=str(tmpd_cwd),
                                            resource_msgs=resource_msgs)

    # make radio

    radio_sender = radio_config.create_sender()

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # verify it comes out of the receiver

    m = resource_msgs.get()

    assert m == message, "The sent message should appear in the queue"

    # Shut down router.
    # In the multiprocessing radio, nothing happens at shutdown, so this
    # validates that the call executes without raising an exception, but
    # not much else.
    receiver.shutdown()
