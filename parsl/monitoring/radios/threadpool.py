from multiprocessing import Queue

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)


class ThreadPoolRadio(RadioConfig):
    def create_sender(self) -> MonitoringRadioSender:
        return ThreadPoolRadioSender(self._queue)

    def create_receiver(self, *, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        # This object is only for use with an in-process thread-pool so it
        # is fine to store a reference to the message queue directly.
        # (TODO: not true - the resource monitor also wants to use this
        # radio config - but maybe that's ok to move across a multiprocessing
        # boundary too?)
        self._queue = resource_msgs
        return ThreadPoolRadioReceiver()


class ThreadPoolRadioSender(MonitoringRadioSender):

    def __init__(self, queue: Queue):
        self._queue = queue

    def send(self, msg: object) -> None:
        self._queue.put(msg)


class ThreadPoolRadioReceiver(MonitoringRadioReceiver):
    def shutdown(self) -> None:
        pass
