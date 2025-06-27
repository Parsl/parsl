import hashlib
import hmac
import logging
import pickle
import secrets
import socket
from multiprocessing.queues import Queue
from typing import Optional

from parsl.monitoring.radios.base import (
    MonitoringRadioReceiver,
    MonitoringRadioSender,
    RadioConfig,
)
from parsl.monitoring.radios.udp_router import start_udp_receiver

logger = logging.getLogger(__name__)


class UDPRadio(RadioConfig):
    def __init__(self, *, port: Optional[int] = None, atexit_timeout: int = 3, address: str, debug: bool = False, hmac_digest: str = 'sha512'):
        self.port = port
        self.atexit_timeout = atexit_timeout
        self.address = address
        self.debug = debug
        self.hmac_digest = hmac_digest

    def create_sender(self) -> MonitoringRadioSender:
        assert self.port is not None, "self.port should have been initialized by create_receiver"
        return UDPRadioSender(self.address, self.port, self.hmac_key, self.hmac_digest)

    def create_receiver(self, run_dir: str, resource_msgs: Queue) -> MonitoringRadioReceiver:
        # RFC 2104 section 2 recommends that the key length be at
        # least as long as the hash output (64 bytes in the case of SHA512).
        # RFC 2014 section 3 talks about periodic key refreshing. This key is
        # not refreshed inside a workflow run, but each separate workflow run
        # uses a new key.
        keysize = hashlib.new(self.hmac_digest).digest_size
        self.hmac_key = secrets.token_bytes(keysize)

        udp_receiver = start_udp_receiver(logdir=run_dir,
                                          monitoring_messages=resource_msgs,
                                          port=self.port,
                                          debug=self.debug,
                                          atexit_timeout=self.atexit_timeout,
                                          hmac_key=self.hmac_key,
                                          hmac_digest=self.hmac_digest
                                          )
        self.port = udp_receiver.port
        return udp_receiver


class UDPRadioSender(MonitoringRadioSender):

    def __init__(self, address: str, port: int, hmac_key: bytes, hmac_digest: str, *, timeout: int = 10) -> None:
        self.sock_timeout = timeout
        self.address = address
        self.port = port
        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest

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
            data = pickle.dumps(message)
            origin_hmac = hmac.digest(self.hmac_key, data, self.hmac_digest)
            buffer = origin_hmac + data
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
