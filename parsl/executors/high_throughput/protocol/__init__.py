from types import MappingProxyType
from typing import Mapping, Protocol, TypeVar

from parsl.executors.high_throughput.protocol import legacy
from parsl.executors.high_throughput.protocol.errors import ProtocolError
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
from parsl.executors.high_throughput.protocol.versions import (
    MAGIC,
    ProtocolVersion,
    UnsupportedProtocolVersion,
    peek_version,
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


class WireFormat(Protocol):
    def encode(self, message: Message) -> bytes:
        ...

    def decode(self, data: bytes, channel: Channel[T]) -> T:
        ...


WIRE_FORMATS: Mapping[ProtocolVersion, WireFormat] = MappingProxyType({"legacy": legacy})


def encode(message: Message) -> bytes:
    return legacy.encode(message)


def decode(data: bytes, channel: Channel[T]) -> T:
    version = peek_version(data)
    wire_format = WIRE_FORMATS.get(version)
    if wire_format is None:
        raise UnsupportedProtocolVersion(version, WIRE_FORMATS)
    return wire_format.decode(data, channel)
