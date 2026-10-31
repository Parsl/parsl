from typing import Optional

import zmq

import parsl.curvezmq as curvezmq
from parsl.monitoring.radios.base import MonitoringRadioSender


class ZMQRadioSender(MonitoringRadioSender):
    """A monitoring radio which connects over ZMQ. This radio is not
    thread-safe, because its use of ZMQ is not thread-safe.
    """

    def __init__(self, hub_address: str, hub_zmq_port: int, cert_dir: Optional[str]) -> None:
        self._hub_channel = curvezmq.ClientContext(cert_dir=cert_dir).socket(zmq.DEALER)
        self._hub_channel.set_hwm(0)
        self._hub_channel.connect(f"tcp://{hub_address}:{hub_zmq_port}")

    def send(self, message: object) -> None:
        self._hub_channel.send_pyobj(message)
