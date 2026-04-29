import logging
import pickle
from multiprocessing.queues import Queue

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class HTEXRadio(RadioConfig, RepresentationMixin):
    def create_sender(self) -> MonitoringRadioSender:
        return HTEXRadioSender()

    def create_receiver(self, *, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        return HTEXRadioReceiver()


class HTEXRadioSender(MonitoringRadioSender):

    def __init__(self) -> None:
        # there is nothing to initialize
        pass

    def send(self, message: object) -> None:
        """ Sends a message via HTEX result channel.

        Parameter
        ---------

        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            None
        """

        import parsl.executors.high_throughput.monitoring_info

        result_queue = parsl.executors.high_throughput.monitoring_info.result_queue

        # this message needs to go in the result queue tagged so that it is treated
        # as a monitoring message by the interchange.

        # for the interchange, the outer wrapper, this needs to be a dict:

        interchange_msg = {
            'type': 'monitoring',
            'payload': message
        }

        if result_queue:
            result_queue.put(pickle.dumps(interchange_msg))
        else:
            logger.error("result_queue is uninitialized - cannot put monitoring message")


class HTEXRadioReceiver(MonitoringRadioReceiver):
    def shutdown(self) -> None:
        # there is nothing to shut down
        pass
