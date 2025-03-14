from abc import ABCMeta, abstractmethod
from multiprocessing.queues import Queue


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
    def create_sender(self) -> MonitoringRadioSender:
        pass

    @abstractmethod
    def create_receiver(self, *, ip: str, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        # TODO: return a shutdownable, and probably take some context to help in
        # creation of the radio config? esp. the ZMQ endpoint to send messages to
        # from the receiving process that might be created?
        """create a receiver for this config, and update this config as
        appropriate so that create_sender will be able to connect back to that
        receiver in whichever way is relevant. create_sender can assume
        that create_receiver has been called."""
        pass
