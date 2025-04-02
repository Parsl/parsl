import pytest

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.radios.filesystem import FilesystemRadioSender
from parsl.monitoring.radios.filesystem_router import start_filesystem_receiver
from parsl.multiprocessing import SpawnQueue


@pytest.mark.local
def test_filesystem(tmpd_cwd):
    """Test filesystem radio/receiver pair.
    This test checks that the pair can be started up locally, that a message
    is conveyed from radio to receiver, and that the receiver process goes
    away at shutdown.
    """

    resource_msgs = SpawnQueue()

    # start receiver
    receiver = start_filesystem_receiver(debug=True,
                                         logdir=str(tmpd_cwd),
                                         monitoring_messages=resource_msgs,
                                         )

    # make radio

    radio_sender = FilesystemRadioSender(run_dir=str(tmpd_cwd),
                                         monitoring_url="irrelevant:")

    # send message into radio

    message = (MessageType.RESOURCE_INFO, {})

    radio_sender.send(message)

    # verify it comes out of the receiver

    m = resource_msgs.get()

    assert m == message, "The sent message should appear in the queue"

    # shut down router

    receiver.close()

    # we can't inspect the process if it has been closed properly, but
    # we can verify that it raises the expected ValueError the closed
    # processes raise on access.
    with pytest.raises(ValueError):
        receiver.process.exitcode
