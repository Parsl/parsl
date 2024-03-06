import datetime
import json
import os
import socket
import pickle
import uuid
import logging

from abc import ABCMeta, abstractmethod

from typing import Optional, Any

from parsl.serialize import serialize

_db_manager_excepts: Optional[Exception]


logger = logging.getLogger(__name__)


class MonitoringRadio(metaclass=ABCMeta):
    @abstractmethod
    def send(self, message: object) -> None:
        pass


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)


class DiasporaRadio(MonitoringRadio):
    def __init__(self, monitoring_url: str, source_id: int, timeout: int = 10):
        from diaspora_event_sdk import KafkaProducer
        self.source_id = source_id
        self.producer = KafkaProducer(value_serializer=DiasporaRadio.serialize)
        logger.info("Diaspora-based monitoring channel initializing")

    def send(self, message: object) -> None:
        topic = "radio-test"
        if isinstance(message, tuple):
            # TODO: make configurable
            if 'run_id' in message[1]:
                key = message[1]['run_id'].encode("utf-8")
            else:
                logger.info("set key as init")
                key = b"init"
            # logger.info(f"Sending message of type {key}:{msg_type} to topic {topic}, content {message[1]}")
            self.producer.send(topic=topic, key=key, value=message[1])
        else:
            key = b"payload"
            self.producer.send(topic=topic, key=key, value=message)
        logger.info("Sent message")
        return

    @staticmethod
    def serialize(value: Any) -> bytes:
        return json.dumps(value, cls=DateTimeEncoder).encode("utf-8")


class FilesystemRadio(MonitoringRadio):
    """A MonitoringRadio that sends messages over a shared filesystem.

    The messsage directory structure is based on maildir,
    https://en.wikipedia.org/wiki/Maildir

    The writer creates a message in tmp/ and then when it is fully
    written, moves it atomically into new/

    The reader ignores tmp/ and only reads and deletes messages from
    new/

    This avoids a race condition of reading partially written messages.

    This radio is likely to give higher shared filesystem load compared to
    the UDPRadio, but should be much more reliable.
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


class HTEXRadio(MonitoringRadio):

    def __init__(self, monitoring_url: str, source_id: int, timeout: int = 10):
        """
        Parameters
        ----------

        monitoring_url : str
            URL of the form <scheme>://<IP>:<PORT>
        source_id : str
            String identifier of the source
        timeout : int
            timeout, default=10s
        """
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


class UDPRadio(MonitoringRadio):

    def __init__(self, monitoring_url: str, source_id: int, timeout: int = 10):
        """
        Parameters
        ----------

        monitoring_url : str
            URL of the form <scheme>://<IP>:<PORT>
        source_id : str
            String identifier of the source
        timeout : int
            timeout, default=10s
        """
        self.monitoring_url = monitoring_url
        self.sock_timeout = timeout
        self.source_id = source_id
        try:
            self.scheme, self.ip, port = (x.strip('/') for x in monitoring_url.split(':'))
            self.port = int(port)
        except Exception:
            raise Exception("Failed to parse monitoring url: {}".format(monitoring_url))

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


def get_monitoring_radio(monitoring_url: str, source_id: int, radio_mode: str, run_dir: str) -> MonitoringRadio:
    if radio_mode == "udp":
        return UDPRadio(monitoring_url, source_id)
    elif radio_mode == "htex":
        return HTEXRadio(monitoring_url, source_id)
    elif radio_mode == "filesystem":
        return FilesystemRadio(monitoring_url=monitoring_url, source_id=source_id, run_dir=run_dir)
    elif radio_mode == "diaspora":
        return DiasporaRadio(monitoring_url, source_id)
    else:
        raise ValueError(f"Unknown radio mode {radio_mode}")
