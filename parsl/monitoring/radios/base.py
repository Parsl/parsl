import logging
import pickle
from abc import ABCMeta, abstractmethod
from multiprocessing.queues import Queue
from typing import Any, Optional

import zmq

_db_manager_excepts: Optional[Exception]

logger = logging.getLogger(__name__)


class MonitoringRadioReceiver(metaclass=ABCMeta):
    @abstractmethod
    def shutdown(self) -> None:
        pass


class MonitoringRadioSender(metaclass=ABCMeta):
    @abstractmethod
    def send(self, message: object) -> None:
        pass


class RadioConfig(metaclass=ABCMeta):
    """Base class for radio plugin configuration.
    """
    @abstractmethod
    def create_sender(self, *, source_id: int) -> MonitoringRadioSender:
        pass

    @abstractmethod
    def create_receiver(self, *, ip: str, resource_msgs: Queue) -> Any:
        # TODO: return a shutdownable, and probably take some context to help in
        # creation of the radio config? esp. the ZMQ endpoint to send messages to
        # from the receiving process that might be created?
        """create a receiver for this config, and update this config as
        appropriate so that create_sender will be able to connect back to that
        receiver in whichever way is relevant. create_sender can assume
        that create_receiver has been called.section 35 of the Opticians Act 1989 provides for a lower quorum of two"""
        pass


class HTEXRadio(RadioConfig):
    def create_sender(self, *, source_id: int) -> MonitoringRadioSender:
        return HTEXRadioSender(source_id=source_id)

    def create_receiver(self, *, ip: str, resource_msgs: Queue) -> None:
        pass


class HTEXRadioSender(MonitoringRadioSender):

    def __init__(self, source_id: int):
        self.source_id = source_id
        logger.info("htex-based monitoring channel initialising")

    def send(self, message: object) -> None:
        """ Sends a message to the UDP receiver

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
        # i) as a monitoring message by the interchange, and then further more treated
        # as a RESOURCE_INFO message when received by monitoring (rather than a NODE_INFO
        # which is the implicit default for messages from the interchange)

        # for the interchange, the outer wrapper, this needs to be a dict:

        interchange_msg = {
            'type': 'monitoring',
            'payload': message
        }

        if result_queue:
            result_queue.put(pickle.dumps(interchange_msg))
        else:
            logger.error("result_queue is uninitialized - cannot put monitoring message")

        return


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
        self.queue.put((message, 0))


class ZMQRadioSender(MonitoringRadioSender):
    """A monitoring radio which connects over ZMQ. This radio is not
    thread-safe, because its use of ZMQ is not thread-safe.
    """

    def __init__(self, hub_address: str, hub_zmq_port: int) -> None:
        self._hub_channel = zmq.Context().socket(zmq.DEALER)
        self._hub_channel.set_hwm(0)
        self._hub_channel.connect(f"tcp://{hub_address}:{hub_zmq_port}")

    def send(self, message: object) -> None:
        self._hub_channel.send_pyobj(message)
