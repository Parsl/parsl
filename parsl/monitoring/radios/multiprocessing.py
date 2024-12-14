from multiprocessing.queues import Queue

from parsl.monitoring.radios.base import MonitoringRadioSender


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
