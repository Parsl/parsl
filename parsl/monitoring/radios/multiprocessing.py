from multiprocessing import Queue

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)


class MultiprocessingQueueRadioSender(MonitoringRadioSender):
    """A monitoring radio which connects over a multiprocessing Queue.
    This radio is intended to be used on the submit side, where components
    in the submit process, or processes launched by multiprocessing, will have
    access to a Queue shared with the monitoring database code (bypassing the
    monitoring router).
    """
    def __init__(self, queue: Queue) -> None:
        self.queue = queue

    def send(self, message: object) -> None:
        self.queue.put(message)


class MultiprocessingQueueRadio(RadioConfig):
    def create_sender(self) -> MonitoringRadioSender:
        return MultiprocessingQueueRadioSender(self._queue)

    def create_receiver(self, *, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        # This object is only for use with an in-process thread-pool so it
        # is fine to store a reference to the message queue directly.
        self._queue = resource_msgs
        return MultiprocessingQueueRadioReceiver()


class MultiprocessingQueueRadioReceiver(MonitoringRadioReceiver):
    def shutdown(self) -> None:
        pass
