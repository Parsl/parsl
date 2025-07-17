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

    This provides the configuration for a particular way of sending monitoring
    messages from a source of monitoring messages into the submit side
    monitoring database.

    This uses staged initialization like lots of Parsl configuration, but in
    a slightly different form.

    A RadioConfig object must be pickleable, because it will be sent to remote
    workers to configure senders. The MonitoringRadioSender and
    MonitoringRadioReceiver objects do not need to be pickleable (and often
    will not be - for example, when they hold references to other processes).

    The RadioConfig object will be used by Parsl in this sequence:

    * A user creates a RadioConfig object from the appropriate subclass for
      radio mechanism they want to use, and specifies it as part of their
      executor configuration.

    Methods on the RadioConfig will then be invoked by Parsl like this:

     * one create_receiver call, on the submit side
        - this call can modify the state of radioconfig to contain information
          about how a sender can connect back to the receiver. for example,
          after binding to a particular port, can store that port so that the
          sender knows which port to connect to.

     * Possibly many serializations to get the RadioConfig to remote workers

     * Many (0 or more) create_sender calls, possibly on remote workers, to
       create the sending side of the radio (MonitoringRadioSender instances)

     * Those senders are used to send messages

     * At executor shutdown, the receiver is shut down.

    This object cannot be re-used across parsl configurations - like many other
    pieces of parsl config it is single use in that respect.
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
