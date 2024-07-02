import logging
import multiprocessing as mp
import os
import pickle
import socket
import threading
import time
import uuid
from abc import ABCMeta, abstractmethod
from multiprocessing.queues import Queue
from typing import Any, Optional, Union

import zmq

from parsl.process_loggers import wrap_with_logs
from parsl.serialize import serialize

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


class UDPRadio(RadioConfig):
    ip: str

    # these two values need to be initialized by a create_receiver step...
    # which is why an earlier patch needs to turn the UDP and zmq receivers
    # into separate threads that can exist separately

    # TODO: this atexit_timeout is now user exposed - in the prior UDP-in-router impl, I think maybe it wasn't (but i should check)
    def __init__(self, *, port: Optional[int] = None, atexit_timeout: Union[int, float] = 3):
        # TODO awkward: when a user creates this it can be none,
        # but after create_receiver initalization it is always an int.
        # perhaps leads to motivation of serializable config being its
        # own class distinct from the user-specified RadioConfig object?
        # Right now, there would be a type-error in create_sender except
        # for an assert that asserts this reasoning to mypy.
        self.port = port
        self.atexit_timeout = atexit_timeout

    def create_sender(self, *, source_id: int) -> MonitoringRadioSender:
        assert self.port is not None, "self.port should have been initialized by create_receiver"
        return UDPRadioSender(self.ip, self.port, source_id)

    def create_receiver(self, ip: str, resource_msgs: Queue) -> Any:
        """TODO: backwards compatibility would require a single one of these to
        exist for all executors that want one, shut down when the last of its
        users asks for shut down... in the case that udp_port is specified.

        But maybe the right thing to do here is lose that configuration parameter
        in that form? especially as I'd like UDPRadio to go away entirely because
        UDP isn't reliable or secure and other code requires reliability of messaging?
        """

        # we could bind to this instead of 0.0.0.0 but that would change behaviour,
        # possibly breaking if the IP address isn't bindable (for example, if its
        # a port forward). Otherwise, it isn't needed for creation of the listening
        # port - only for creation of the sender.
        self.ip = ip

        udp_sock = socket.socket(socket.AF_INET,
                                 socket.SOCK_DGRAM,
                                 socket.IPPROTO_UDP)

        # We are trying to bind to all interfaces with 0.0.0.0
        if self.port is None:
            udp_sock.bind(('0.0.0.0', 0))
            self.port = udp_sock.getsockname()[1]
        else:
            try:
                udp_sock.bind(('0.0.0.0', self.port))
            except Exception as e:
                # TODO: this can be its own patch to use 'from' notation?
                raise RuntimeError(f"Could not bind to UDP port {self.port}") from e
        udp_sock.settimeout(0.001)  # TODO: configurable loop_freq? it's hard-coded though...
        logger.info(f"Initialized the UDP socket on port {self.port}")

        # this is now in the submitting process, not the router process.
        # I don't think this matters for UDP so much because it's on the
        # way out - but how should this work for other things? compare with
        # filesystem radio impl?
        logger.info("Starting UDP listener thread")
        udp_radio_receiver_thread = UDPRadioReceiverThread(udp_sock=udp_sock, resource_msgs=resource_msgs, atexit_timeout=self.atexit_timeout)
        udp_radio_receiver_thread.start()

        return udp_radio_receiver_thread
        # TODO: wrap this with proper shutdown logic involving events etc?


class UDPRadioReceiverThread(threading.Thread):
    def __init__(self, udp_sock: socket.socket, resource_msgs: Queue, atexit_timeout: Union[int, float]):
        self.exit_event = mp.Event()
        self.udp_sock = udp_sock
        self.resource_msgs = resource_msgs
        self.atexit_timeout = atexit_timeout
        super().__init__()

    @wrap_with_logs
    def run(self) -> None:
        try:
            while not self.exit_event.is_set():
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    resource_msg = pickle.loads(data)
                    logger.debug("Got UDP Message from {}: {}".format(addr, resource_msg))
                    self.resource_msgs.put((resource_msg, addr))
                except socket.timeout:
                    pass

            logger.info("UDP listener draining")
            last_msg_received_time = time.time()
            while time.time() - last_msg_received_time < self.atexit_timeout:
                try:
                    data, addr = self.udp_sock.recvfrom(2048)
                    msg = pickle.loads(data)
                    logger.debug("Got UDP Message from {}: {}".format(addr, msg))
                    self.resource_msgs.put((msg, addr))
                    last_msg_received_time = time.time()
                except socket.timeout:
                    pass

            logger.info("UDP listener finishing normally")
        finally:
            logger.info("UDP listener finished")

    def shutdown(self) -> None:
        logger.debug("Set exit event")
        self.exit_event.set()
        logger.debug("Joining")
        self.join()
        logger.debug("done")


class HTEXRadio(RadioConfig):
    def create_sender(self, *, source_id: int) -> MonitoringRadioSender:
        return HTEXRadioSender(source_id=source_id)

    def create_receiver(self, *, ip: str, resource_msgs: Queue) -> None:
        pass


class FilesystemRadioSender(MonitoringRadioSender):
    """A MonitoringRadioSender that sends messages over a shared filesystem.

    The messsage directory structure is based on maildir,
    https://en.wikipedia.org/wiki/Maildir

    The writer creates a message in tmp/ and then when it is fully
    written, moves it atomically into new/

    The reader ignores tmp/ and only reads and deletes messages from
    new/

    This avoids a race condition of reading partially written messages.

    This radio is likely to give higher shared filesystem load compared to
    the UDP radio, but should be much more reliable.
    """

    def __init__(self, *, monitoring_url: str, source_id: int, timeout: int = 10, run_dir: str):
        logger.info("filesystem based monitoring channel initializing")
        self.source_id = source_id
        self.base_path = f"{run_dir}/monitor-fs-radio/"
        self.tmp_path = f"{self.base_path}/tmp"
        self.new_path = f"{self.base_path}/new"

        os.makedirs(self.tmp_path, exist_ok=True)
        os.makedirs(self.new_path, exist_ok=True)

    def send(self, message: object) -> None:
        logger.info("Sending a monitoring message via filesystem")

        unique_id = str(uuid.uuid4())

        tmp_filename = f"{self.tmp_path}/{unique_id}"
        new_filename = f"{self.new_path}/{unique_id}"
        buffer = (message, "NA")

        # this will write the message out then atomically
        # move it into new/, so that a partially written
        # file will never be observed in new/
        with open(tmp_filename, "wb") as f:
            f.write(serialize(buffer))
        os.rename(tmp_filename, new_filename)


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


class UDPRadioSender(MonitoringRadioSender):

    def __init__(self, ip: str, port: int, source_id: int, timeout: int = 10) -> None:
        """
        Parameters
        ----------

        XXX TODO
        monitoring_url : str
            URL of the form <scheme>://<IP>:<PORT>
        source_id : str
            String identifier of the source
        timeout : int
            timeout, default=10s
        """
        self.sock_timeout = timeout
        self.source_id = source_id
        self.ip = ip
        self.port = port

        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)  # UDP
        self.sock.settimeout(self.sock_timeout)

    def send(self, message: object) -> None:
        """ Sends a message to the UDP receiver

        Parameter
        ---------

        message: object
            Arbitrary pickle-able object that is to be sent

        Returns:
            None
        """
        try:
            buffer = pickle.dumps(message)
        except Exception:
            logging.exception("Exception during pickling", exc_info=True)
            return

        try:
            self.sock.sendto(buffer, (self.ip, self.port))
        except socket.timeout:
            logging.error("Could not send message within timeout limit")
            return
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
        print("in zmq radio init. about to log.")
        logger.debug("Creating ZMQ socket")
        print("in zmq radio init. logged first log. about to create context.")
        self._hub_channel = zmq.Context().socket(zmq.DEALER)
        print("in zmq radio init. created context.")
        self._hub_channel.set_hwm(0)
        self._hub_channel.connect(f"tcp://{hub_address}:{hub_zmq_port}")
        logger.debug("Created ZMQ socket")

    def send(self, message: object) -> None:
        self._hub_channel.send_pyobj(message)
