import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.filesystem import FilesystemRadio
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_filesystem(tmpd_cwd):
    """Test filesystem radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    radio_config = FilesystemRadio()

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

    # shut down router

    receiver.shutdown()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        receiver.process.exitcode
