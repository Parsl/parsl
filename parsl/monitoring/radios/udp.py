import logging
import multiprocessing as mp
import pickle
import socket
import threading
import time
from multiprocessing.queues import Queue
from typing import Any, Optional, Union

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)
from parsl.process_loggers import wrap_with_logs

logger = logging.getLogger(__name__)


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
        logger.info("Starting UDP radio message send")
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
        logger.info("Normal ending for UDP radio message send")
        return


class UDPRadioReceiverThread(threading.Thread, MonitoringRadioReceiver):
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
