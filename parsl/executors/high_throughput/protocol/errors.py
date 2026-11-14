import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.executors.high_throughput.protocol import ProtocolVersion


class ProtocolError(Exception):
    pass


class UnsupportedProtocolVersion(ProtocolError):
    def __init__(self, version: "ProtocolVersion") -> None:
        self.version = version

    def __str__(self) -> str:
        from parsl.executors.high_throughput.protocol import WIRE_FORMATS
        supported = sorted(WIRE_FORMATS, key=lambda v: -math.inf if isinstance(v, str) else v)
        return (
            f"HTEX protocol version {self.version} is not supported;"
            f" this copy of Parsl supports: {', '.join(map(str, supported))}"
        )
