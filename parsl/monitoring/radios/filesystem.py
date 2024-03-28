import logging
import os
import pickle
import uuid
from multiprocessing.queues import Queue

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)
from parsl.monitoring.radios.filesystem_router import FilesystemRadioReceiver

logger = logging.getLogger(__name__)


class FilesystemRadio(RadioConfig):
    """A MonitoringRadioSender that sends messages over a shared filesystem.

    The messsage directory structure is based on maildir,
    https://en.wikipedia.org/wiki/Maildir

    The writer creates a message in tmp/ and then when it is fully
    written, moves it atomically into new/

    The reader ignores tmp/ and only reads and deletes messages from
    new/

    This avoids a race condition of reading partially written messages.

    This radio is likely to give higher shared filesystem load compared to
    the UDP radio, but should be much more reliable.
    """

    def create_sender(self) -> MonitoringRadioSender:
        return FilesystemRadioSender(run_dir=self.run_dir)

    def create_receiver(self, *, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        self.run_dir = run_dir
        return FilesystemRadioReceiver(resource_msgs, run_dir)


class FilesystemRadioSender(MonitoringRadioSender):
    def __init__(self, *, run_dir: str):
        logger.info("filesystem based monitoring radio initializing")
        self.base_path = f"{run_dir}/monitor-fs-radio/"
        self.tmp_path = f"{self.base_path}/tmp"
        self.new_path = f"{self.base_path}/new"

        os.makedirs(self.tmp_path, exist_ok=True)
        os.makedirs(self.new_path, exist_ok=True)

    def send(self, message: object) -> None:
        logger.info("Sending a monitoring message via filesystem")

        unique_id = str(uuid.uuid4())

        tmp_filename = f"{self.tmp_path}/{unique_id}"
        new_filename = f"{self.new_path}/{unique_id}"
        buffer = message

        # this will write the message out then atomically
        # move it into new/, so that a partially written
        # file will never be observed in new/
        with open(tmp_filename, "wb") as f:
            pickle.dump(buffer, f)
        os.rename(tmp_filename, new_filename)
