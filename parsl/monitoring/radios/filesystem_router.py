from __future__ import annotations

import logging
import os
import pickle
import time
from multiprocessing.context import SpawnProcess
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from typing import cast

from parsl.log_utils import set_file_logger
from parsl.monitoring.radios.multiprocessing import MultiprocessingQueueRadioSender
from parsl.monitoring.types import TaggedMonitoringMessage
from parsl.multiprocessing import SpawnEvent, join_terminate_close_proc
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

logger = logging.getLogger(__name__)


@wrap_with_logs
def filesystem_router_starter(*, q: Queue[TaggedMonitoringMessage], run_dir: str, exit_event: Event) -> None:
    set_file_logger(f"{run_dir}/monitoring_filesystem_radio.log",
                    level=logging.INFO)

    logger.info("Starting filesystem radio receiver")
    setproctitle("parsl: monitoring filesystem receiver")
    base_path = f"{run_dir}/monitor-fs-radio/"
    tmp_dir = f"{base_path}/tmp/"
    new_dir = f"{base_path}/new/"
    logger.debug("Creating new and tmp paths under %s", base_path)

    target_radio = MultiprocessingQueueRadioSender(q)

    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(new_dir, exist_ok=True)

    while not exit_event.is_set():
        logger.debug("Start filesystem radio receiver loop")

        # iterate over files in new_dir
        for filename in os.listdir(new_dir):
            try:
                logger.info("Processing filesystem radio file %s", filename)
                full_path_filename = f"{new_dir}/{filename}"
                with open(full_path_filename, "rb") as f:
                    message = pickle.load(f)
                logger.debug("Message received is: %s", message)
                assert isinstance(message, tuple)
                target_radio.send(cast(TaggedMonitoringMessage, message))
                os.remove(full_path_filename)
            except Exception:
                logger.exception("Exception processing %s - probably will be retried next iteration", filename)

        time.sleep(1)  # whats a good time for this poll?
    logger.info("Ending filesystem radio receiver")


class FilesystemRadioReceiver():
    def __init__(self, *, process: SpawnProcess, exit_event: Event) -> None:
        self.process = process
        self.exit_event = exit_event

    def close(self) -> None:
        self.exit_event.set()
        join_terminate_close_proc(self.process)


def start_filesystem_receiver(*,
                              monitoring_messages: Queue,
                              logdir: str,
                              debug: bool) -> FilesystemRadioReceiver:

    router_exit_event = SpawnEvent()

    filesystem_proc = SpawnProcess(target=filesystem_router_starter,
                                   kwargs={"q": monitoring_messages,
                                           "run_dir": logdir,
                                           "exit_event": router_exit_event},
                                   name="Monitoring-Filesystem-Process",
                                   daemon=True
                                   )
    filesystem_proc.start()

    return FilesystemRadioReceiver(process=filesystem_proc, exit_event=router_exit_event)
