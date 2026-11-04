import struct
from types import MappingProxyType
from typing import Literal, Mapping, Protocol, TypeVar

from parsl.executors.high_throughput.protocol import legacy
from parsl.executors.high_throughput.protocol.errors import (
    ProtocolError,
    UnsupportedProtocolVersion,
)
from parsl.executors.high_throughput.protocol.messages import (
    INTERCHANGE_TO_EXECUTOR,
    INTERCHANGE_TO_MANAGER,
    MANAGER_TO_INTERCHANGE,
    Channel,
    ConnectionProbe,
    ConnectionProbeReply,
    Drain,
    Heartbeat,
    HeartbeatReply,
    InterchangeFailure,
    Message,
    Monitoring,
    Registration,
    ResourceSpec,
    Result,
    Shutdown,
    Task,
    TaskContext,
    TaskExecutor,
    Tasks,
)

__all__ = [
    'INTERCHANGE_TO_EXECUTOR',
    'INTERCHANGE_TO_MANAGER',
    'MAGIC',
    'MANAGER_TO_INTERCHANGE',
    'WIRE_FORMATS',
    'Channel',
    'ConnectionProbe',
    'ConnectionProbeReply',
    'Drain',
    'Heartbeat',
    'HeartbeatReply',
    'InterchangeFailure',
    'Message',
    'Monitoring',
    'ProtocolError',
    'ProtocolVersion',
    'Registration',
    'ResourceSpec',
    'Result',
    'Shutdown',
    'Task',
    'TaskContext',
    'TaskExecutor',
    'Tasks',
    'UnsupportedProtocolVersion',
    'WireFormat',
    'decode',
    'encode',
    'peek_version',
]

T = TypeVar("T")

MAGIC = b"PHTX"
_PREFIX = struct.Struct("!4sH")

ProtocolVersion = int | Literal["legacy"]


class WireFormat(Protocol):
    def encode(self, message: Message) -> bytes:
        ...

    def decode(self, data: bytes, channel: Channel[T]) -> T:
        ...


WIRE_FORMATS: Mapping[ProtocolVersion, WireFormat] = MappingProxyType({"legacy": legacy})


def encode(message: Message) -> bytes:
    return legacy.encode(message)


def peek_version(data: bytes) -> ProtocolVersion:
    if not data.startswith(MAGIC):
        return "legacy"
    if len(data) < _PREFIX.size:
        raise ProtocolError(f"Truncated HTEX protocol prefix ({len(data)} bytes)")
    _, version = _PREFIX.unpack_from(data)
    return version


def decode(data: bytes, channel: Channel[T]) -> T:
    version = peek_version(data)
    wire_format = WIRE_FORMATS.get(version)
    if wire_format is None:
        raise UnsupportedProtocolVersion(version)
    return wire_format.decode(data, channel)
