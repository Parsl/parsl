import json
import logging
import multiprocessing.queues
import os
import queue
import threading
from typing import Union

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.monitoring import MonitoringHubInterface
from parsl.monitoring.types import TaggedMonitoringMessage
from parsl.multiprocessing import SpawnQueue
from parsl.process_loggers import wrap_with_logs


class JSONHub(MonitoringHubInterface):
    """A MonitoringHub-plugin for logging to JSON-per-line format.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.resource_msgs: multiprocessing.queues.Queue[TaggedMonitoringMessage]
        self.resource_msgs = SpawnQueue()
        self.workflow_name = None
        self.workflow_version = None
        self.resource_monitoring_enabled = True
        self.resource_monitoring_interval = 1
        self.monitoring_debug = True

        self.event = threading.Event()

    def start(self, dfk_run_dir: str, config_run_dir: Union[str, os.PathLike]) -> None:
        self.thread = threading.Thread(target=thread_body, args=(self.resource_msgs, dfk_run_dir, self.event, self.logger), daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.logger.info("closing")
        self.event.set()
        self.logger.info("event set - joining")
        self.thread.join()
        self.logger.info("joined")


@wrap_with_logs
def thread_body(qu: multiprocessing.queues.Queue[TaggedMonitoringMessage], dir: str, event: threading.Event, logger: logging.Logger) -> None:
    with open(dir + "/monitoring.json", "a") as f:
        while not event.is_set():
            logger.info("start of jsonhub loop")
            try:
                q = qu.get(timeout=5)
                logger.info("got q")
                if q[0] == MessageType.RESOURCE_INFO:
                    # the monitoring message is not directly JSON dumpable,
                    # so reformat.
                    m = {}

                    # these three are enough to let per-task tooling find the event
                    m['parsl_dfk'] = q[1]['run_id']
                    m['parsl_task_id'] = q[1]['task_id']
                    m['parsl_try_id'] = q[1]['try_id']
                    m['first_msg'] = q[1]['first_msg']
                    m['last_msg'] = q[1]['last_msg']
                    m['created'] = q[1]['timestamp'].timestamp()

                    m['formatted'] = repr(q[1])  # TODO: this is ugly but it will do for now...

                    json.dump(m, fp=f)
                    print("", file=f)
                    f.flush()
                    logger.info("dumped and flushed")
            except queue.Empty:
                logger.info("empty")
                pass
            logger.info("end of jsonhub loop")
