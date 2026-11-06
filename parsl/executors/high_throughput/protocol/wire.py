import struct
from typing import TypeVar

from parsl.executors.high_throughput.protocol import v0
from parsl.executors.high_throughput.protocol.errors import (
    ProtocolError,
    UnsupportedProtocolVersion,
)
from parsl.executors.high_throughput.protocol.messages import Channel, Message

MAGIC = b"PHTX"
_PREFIX = struct.Struct("!4sH")

SUPPORTED_VERSIONS = frozenset({0})

T = TypeVar("T")


def to_wire(message: Message) -> bytes:
    return v0.encode(message)


def wire_version(data: bytes) -> int:
    if not data.startswith(MAGIC):
        return 0
    if len(data) < _PREFIX.size:
        raise ProtocolError(f"Truncated HTEX protocol prefix ({len(data)} bytes)")
    _, version = _PREFIX.unpack_from(data)
    return version


def from_wire(data: bytes, channel: Channel[T]) -> T:
    if not data.startswith(MAGIC):
        return v0.decode(data, channel)
    raise UnsupportedProtocolVersion(wire_version(data))
