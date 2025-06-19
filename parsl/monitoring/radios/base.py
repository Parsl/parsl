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

    This uses staged initialization like lots of Parsl configuration, but in
    a slightly different form.

    must be pickleable

    * first user creates this object as part of their parsl config, specifying
      user-specified configuration attributes.

    then will be invoked by parsl core like this:

     * one create_receiver
        - this call can modify the state of radioconfig to contain information
          about how a sender can connect back to the receiver. for example,
          after binding to a particular port, can store that port so that the
          sender knows which port to connect to.

     * possibly many serializations; many (0 or more) create_sender calls

     * senders are used to send messages

     * receiver is shut down at the end. (are senders shut down at end too? there's no abstract close call so ... no)

    This object cannot be re-used across parsl configurations - like many other
    pieces of parsl config it is single use in that respect.

    the sender and receiver do not need to be pickleable.
    """

    @abstractmethod
    def create_receiver(self, *, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        """Create a receiver for this RadioConfig, and update this RadioConfig
        with enough context to create senders.
        """
        pass

    @abstractmethod
    def create_sender(self) -> MonitoringRadioSender:
        """Create a sender to connect to the receiver created by an
        earlier call to create_receiver.
        """
        pass
