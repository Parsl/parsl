import logging
import pickle
import socket
from multiprocessing.queues import Queue
from typing import Any, Optional, Union

from parsl.monitoring.radios.base import MonitoringRadioSender, RadioConfig
from parsl.monitoring.radios.udp_router import start_udp_receiver

logger = logging.getLogger(__name__)


class UDPRadio(RadioConfig):
    def __init__(self, *, port: Optional[int] = None, atexit_timeout: Union[int, float] = 3, address: str, debug: bool = False):
        self.port = port
        self.atexit_timeout = atexit_timeout
        self.address = address
        self.debug = debug

    def create_sender(self) -> MonitoringRadioSender:
        assert self.port is not None, "self.port should have been initialized by create_receiver"
        return UDPRadioSender(self.address, self.port)

    def create_receiver(self, run_dir: str, resource_msgs: Queue) -> Any:
        udp_receiver = start_udp_receiver(logdir=run_dir,
                                          monitoring_messages=resource_msgs,
                                          port=self.port,
                                          debug=self.debug
                                          )
        self.port = udp_receiver.port
        return udp_receiver


class UDPRadioSender(MonitoringRadioSender):

    def __init__(self, address: str, port: int, timeout: int = 10) -> None:
        self.sock_timeout = timeout
        self.address = address
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
            self.sock.sendto(buffer, (self.address, self.port))
        except socket.timeout:
            logging.error("Could not send message within timeout limit")
            return
        logger.info("Normal ending for UDP radio message send")
        return
