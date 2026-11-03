"""The HTEX protocol's wire format: converts :class:`Message`\\ s to and from
bytes, with :meth:`Message.to_wire` and :func:`from_wire`.

An encoded message is a single ``bytes`` object::

    +-------+---------+------------+--------------+-----------+-----------+-----
    | magic | version | header_len |    header    | buffer 0  | buffer 1  | ...
    |  4 B  |  u16    |    u32     | (header_len) |           |           |
    +-------+---------+------------+--------------+-----------+-----------+-----

- ``magic`` is :data:`MAGIC`; ``version`` and ``header_len`` are big-endian
  unsigned integers. These first 10 bytes will not change in future protocol
  versions, so any version can identify another version's messages.
- ``header`` is a UTF-8 JSON object ``{"m": <message>, "b": [<len>, ...]}``,
  where ``b`` lists the lengths of the buffers that follow, in order. JSON
  numbers may include ``Infinity``, ``-Infinity``, and ``NaN``, as written and
  read by Python's :mod:`json` module.
- ``m`` holds the message's ``type`` tag and one key per field. Nested
  messages (e.g. the tasks of a :class:`~.messages.Tasks`) are JSON objects
  of their fields, without a ``type`` tag.

JSON cannot hold bytes, so each ``bytes`` field is moved, as is, to the list
of buffers, and replaced in ``m`` by its index in that list. Bytes positions
are known from the field annotations rather than marked in the data, so
arbitrary JSON values (e.g. user-provided keyword arguments) cannot be
mistaken for buffer references. The annotations understood are ``bytes``,
``Optional[...]``, nested messages, and ``List[...]``; any other field is a
plain JSON value, which may not hold ``bytes``.

Messages are not validated beyond the magic bytes and version: peers are
other HTEX components, and messages are type-checked where they are built. A
malformed message fails to decode with a :class:`ProtocolError`, chained to
whatever went wrong.
"""
import dataclasses
import json
import struct
from types import NoneType, UnionType
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import dataclass_transform, get_args, get_origin

from parsl.executors.high_throughput.protocol.errors import (
    ProtocolError,
    UnsupportedProtocolVersion,
)

#: The protocol version this copy of Parsl sends.
PROTOCOL_VERSION = 1

#: The oldest protocol version this copy of Parsl can decode. Raise it to
#: :data:`PROTOCOL_VERSION` when making a change older versions cannot handle.
OLDEST_SUPPORTED_VERSION = 1

MAGIC = b"PHTX"
_PREFIX = struct.Struct("!4sHI")

#: Message ``type`` tag -> its class, for every :class:`Message` subclass.
MESSAGES: Dict[str, Type["Message"]] = {}


# dataclass_transform makes subclasses dataclasses (to mypy); @dataclass makes
# the base one too, so mypy accepts dataclasses.fields() on any Message class
@dataclass_transform(kw_only_default=True, frozen_default=True)
@dataclasses.dataclass(frozen=True, kw_only=True)
class Message:
    """Base class of all messages; see :mod:`~.messages`.

    Subclasses must set ``type``, the tag that names them on the wire, and
    are made into frozen, keyword-only dataclasses automatically. A subclass
    declared with ``abstract=True`` (e.g. :class:`~.messages.Command`) is a
    base for other messages: it has no tag and is never sent itself.
    """
    type: ClassVar[str]

    def __init_subclass__(cls, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        dataclasses.dataclass(frozen=True, kw_only=True)(cls)
        if abstract:
            return
        tag = cls.__dict__.get("type")
        if not isinstance(tag, str):
            raise TypeError(f"{cls.__name__} must set a `type` tag")
        if tag in MESSAGES:
            raise TypeError(f"{cls.__name__}: `type` tag {tag!r} is already used by {MESSAGES[tag].__name__}")
        MESSAGES[tag] = cls

    def to_wire(self) -> bytes:
        """Encode this message; see :func:`to_wire`."""
        return to_wire(self)


def _optional_of(annotation: Any) -> Optional[Any]:
    # return X if annotation is Optional[X], else None
    if get_origin(annotation) in (Union, UnionType):
        args = [a for a in get_args(annotation) if a is not NoneType]
        if len(args) == 1:
            return args[0]
    return None


def to_wire(message: Message) -> bytes:
    """Encode a message.

    Field values are not checked here (mypy checks them where messages are
    built); a non-bytes value in a ``bytes`` field fails with ``TypeError``.

    :raises ProtocolError: if the message holds a value JSON cannot encode
    """
    buffers: List[bytes] = []

    def encode(annotation: Any, value: Any) -> Any:
        if isinstance(annotation, type) and issubclass(annotation, Message):
            return {
                f.name: encode(f.type, getattr(value, f.name))
                for f in dataclasses.fields(annotation)
            }

        if annotation is bytes:
            buffers.append(value)
            return len(buffers) - 1

        if (inner := _optional_of(annotation)) is not None:
            return None if value is None else encode(inner, value)

        if get_origin(annotation) is list:
            (item_type,) = get_args(annotation)
            return [encode(item_type, v) for v in value]

        return value

    tag = message.type
    fields = encode(type(message), message)
    lengths = [len(b) for b in buffers]
    try:
        header = json.dumps({"m": {"type": tag, **fields}, "b": lengths}, separators=(",", ":")).encode()
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"{tag}: {e}") from e
    return b"".join([_PREFIX.pack(MAGIC, PROTOCOL_VERSION, len(header)), header, *buffers])


def peek_version(data: bytes) -> int:
    """Return the protocol version of an encoded message, without decoding it.

    :raises ProtocolError: if ``data`` is not an HTEX protocol message
    """
    if len(data) < _PREFIX.size:
        raise ProtocolError(f"Message too short ({len(data)} bytes)")
    magic, version, _ = _PREFIX.unpack_from(data)
    if magic != MAGIC:
        raise ProtocolError("Not an HTEX protocol message (bad magic bytes)")
    return version


M = TypeVar("M", bound=Message)
M1 = TypeVar("M1", bound=Message)
M2 = TypeVar("M2", bound=Message)
M3 = TypeVar("M3", bound=Message)
M4 = TypeVar("M4", bound=Message)
M5 = TypeVar("M5", bound=Message)
M6 = TypeVar("M6", bound=Message)


# One overload per tuple length, so mypy sees a channel tuple's classes as a
# union, e.g. for exhaustive `match`es.
# A longer channel tuple is a mypy error until it gets an overload here.
@overload
def from_wire(data: bytes) -> Message:
    ...


@overload
def from_wire(data: bytes, expected: Type[M]) -> M:
    ...


@overload
def from_wire(data: bytes, expected: Tuple[Type[M1], Type[M2]]) -> Union[M1, M2]:
    ...


@overload
def from_wire(data: bytes, expected: Tuple[Type[M1], Type[M2], Type[M3]]) -> Union[M1, M2, M3]:
    ...


@overload
def from_wire(
    data: bytes, expected: Tuple[Type[M1], Type[M2], Type[M3], Type[M4]]
) -> Union[M1, M2, M3, M4]:
    ...


@overload
def from_wire(
    data: bytes, expected: Tuple[Type[M1], Type[M2], Type[M3], Type[M4], Type[M5]]
) -> Union[M1, M2, M3, M4, M5]:
    ...


@overload
def from_wire(
    data: bytes, expected: Tuple[Type[M1], Type[M2], Type[M3], Type[M4], Type[M5], Type[M6]]
) -> Union[M1, M2, M3, M4, M5, M6]:
    ...


def from_wire(
    data: bytes, expected: Union[Type[Message], Tuple[Type[Message], ...], None] = None
) -> Message:
    """Decode a message.

    :param data: an encoded message, as returned by :meth:`Message.to_wire`
    :param expected: if given, the message class, or tuple of classes (e.g.
        :data:`~.messages.MANAGER_TO_INTERCHANGE`), allowed here; other types
        raise ``ProtocolError``
    :raises UnsupportedProtocolVersion: if the message's protocol version is
        older than :data:`OLDEST_SUPPORTED_VERSION`
    :raises ProtocolError: if the message cannot be decoded
    """
    version = peek_version(data)
    if version < OLDEST_SUPPORTED_VERSION:
        raise UnsupportedProtocolVersion(version, OLDEST_SUPPORTED_VERSION)

    def decode(annotation: object, value: Any) -> Any:
        if isinstance(annotation, type) and issubclass(annotation, Message):
            fields = {f.name: f.type for f in dataclasses.fields(annotation)}
            return annotation(**{k: decode(fields[k], v) for k, v in value.items()})

        if annotation is bytes:
            return buffers[value]

        if (inner := _optional_of(annotation)) is not None:
            return None if value is None else decode(inner, value)

        if get_origin(annotation) is list:
            (item_type,) = get_args(annotation)
            return [decode(item_type, v) for v in value]

        return value

    try:
        _, _, header_len = _PREFIX.unpack_from(data)
        offset = _PREFIX.size + header_len
        header = json.loads(data[_PREFIX.size:offset])
        buffers = []
        for n in header["b"]:
            buffers.append(data[offset:offset + n])
            offset += n
        message = header["m"]
        tag = message.pop("type")
        cls = MESSAGES.get(tag)
    except Exception as e:
        raise ProtocolError(f"Malformed message: {e!r}") from e

    if isinstance(expected, type):
        expected = (expected,)
    if cls is None or (expected is not None and cls not in expected):
        raise ProtocolError(f"Unexpected message type: {tag!r}")

    try:
        return decode(cls, message)
    except Exception as e:
        raise ProtocolError(f"Malformed message: {e!r}") from e
