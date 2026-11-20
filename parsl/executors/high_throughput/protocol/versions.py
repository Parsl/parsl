import math
import struct
from typing import Iterable, Literal

from parsl.executors.high_throughput.protocol.errors import ProtocolError

ProtocolVersion = int | Literal["legacy"]

MAGIC = b"PHTX"
PREFIX = struct.Struct("!4sH")

OLDEST_SUPPORTED_PARSL = "2026.10.05"


class UnsupportedProtocolVersion(ProtocolError):
    def __init__(self, version: ProtocolVersion, supported: Iterable[ProtocolVersion]) -> None:
        self.version = version
        self.supported = sorted(supported, key=_version_order)

    def __str__(self) -> str:
        return (
            f"HTEX protocol version {self.version} is not supported;"
            f" this copy of Parsl supports: {', '.join(map(str, self.supported))}"
        )


def _version_order(version: ProtocolVersion) -> float:
    return -math.inf if isinstance(version, str) else version


def peek_version(data: bytes) -> ProtocolVersion:
    if not data.startswith(MAGIC):
        return "legacy"
    if len(data) < PREFIX.size:
        raise ProtocolError(f"Truncated HTEX protocol prefix ({len(data)} bytes)")
    _, version = PREFIX.unpack_from(data)
    return version
