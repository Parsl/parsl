import importlib
import logging
import uuid
from typing import Any, Dict, List, Optional, Union

import parsl.serialize.concretes as concretes
from parsl.serialize.base import SerializerBase
from parsl.serialize.errors import DeserializerPluginError
from parsl.trace import Span, event, span_bind_sub

logger = logging.getLogger(__name__)


methods_for_code: Dict[bytes, SerializerBase] = {}


def register_method_for_code(s: SerializerBase) -> None:
    methods_for_code[s.identifier] = s


register_method_for_code(concretes.DillCallableSerializer())


methods_for_data: Dict[bytes, SerializerBase] = {}


def register_method_for_data(s: SerializerBase) -> None:
    methods_for_data[s.identifier] = s


register_method_for_data(concretes.PickleSerializer())
register_method_for_data(concretes.DillSerializer())


# When deserialize dynamically loads a deserializer, it will be stored here,
# rather than in the methods_for_* dictionaries, so that loading does not
# cause it to be used for future serializations.
additional_methods_for_deserialization: Dict[bytes, SerializerBase] = {}


def pack_apply_message(func: Any,
                       args: Any,
                       kwargs: Any,
                       buffer_threshold: int = int(128 * 1e6),
                       super_span: Optional[Span] = None) -> bytes:
    """Serialize and pack function and parameters

    Parameters
    ----------

    func: Function
        A function to ship

    args: Tuple/list of objects
        positional parameters as a list

    kwargs: Dict
        Dict containing named parameters

    buffer_threshold: int
        Limits buffer to specified size in bytes. Exceeding this limit would give you
        a warning in the log. Default is 128MB.
    """
    pack_apply_id = str(uuid.uuid4())
    pack_apply_span = Span("PACKAPPLY", pack_apply_id)
    if super_span is not None:
        span_bind_sub(super_span, pack_apply_span)

    event("SERIALIZE_PACK_APPLY_FUNC", pack_apply_span)
    b_func = serialize(func, buffer_threshold=buffer_threshold)

    event("SERIALIZE_PACK_APPLY_ARGS", pack_apply_span)
    b_args = serialize(args, buffer_threshold=buffer_threshold)

    event("SERIALIZE_PACK_APPLY_KWARGS", pack_apply_span)
    b_kwargs = serialize(kwargs, buffer_threshold=buffer_threshold)

    event("SERIALIZE_PACK_APPLY_PACK_BUFFERS", pack_apply_span)
    packed_buffer = pack_buffers([b_func, b_args, b_kwargs])

    event("SERIALIZE_PACK_APPLY_END", pack_apply_span)
    return packed_buffer


def pack_res_spec_apply_message(func: Any, args: Any, kwargs: Any, resource_specification: Any, buffer_threshold: int = int(128 * 1e6)) -> bytes:
    """Serialize and pack function, parameters, and resource_specification

    Parameters
    ----------

    func: Function
        A function to ship

    args: Tuple/list of objects
        positional parameters as a list

    kwargs: Dict
        Dict containing named parameters

    resource_specification: Dict
        Dict containing application resource specification

    buffer_threshold: int
        Limits buffer to specified size in bytes. Exceeding this limit would give you
        a warning in the log. Default is 128MB.
    """
    return pack_apply_message(func, args, (kwargs, resource_specification), buffer_threshold=buffer_threshold)


def unpack_apply_message(packed_buffer: bytes, user_ns: Any = None, copy: Any = False) -> List[Any]:
    """ Unpack and deserialize function and parameters
    """
    return [deserialize(buf) for buf in unpack_buffers(packed_buffer)]


def unpack_res_spec_apply_message(packed_buffer: bytes, user_ns: Any = None, copy: Any = False) -> List[Any]:
    """ Unpack and deserialize function, parameters, and resource_specification
    """
    func, args, (kwargs, resource_spec) = unpack_apply_message(packed_buffer, user_ns=user_ns, copy=copy)
    return [func, args, kwargs, resource_spec]


def serialize(obj: Any, buffer_threshold: int = int(1e6)) -> bytes:
    """ Try available serialization methods one at a time

    Individual serialization methods might raise a TypeError (eg. if objects are non serializable)
    This method will raise the exception from the last method that was tried, if all methods fail.
    """
    result: Union[bytes, Exception]
    if callable(obj):
        methods = methods_for_code
    else:
        methods = methods_for_data

    for method in methods.values():
        try:
            result = method.identifier + b'\n' + method.serialize(obj)
        except Exception as e:
            result = e
            continue
        else:
            break

    if isinstance(result, BaseException):
        raise result
    else:
        if len(result) > buffer_threshold:
            logger.warning(f"Serialized object exceeds buffer threshold of {buffer_threshold} bytes, this could cause overflows")
        return result


def deserialize(payload: bytes) -> Any:
    """
    Parameters
    ----------
    payload : str
       Payload object to be deserialized

    """
    header, body = payload.split(b'\n', 1)

    if header in methods_for_code:
        deserializer = methods_for_code[header]
    elif header in methods_for_data:
        deserializer = methods_for_data[header]
    elif header in additional_methods_for_deserialization:
        deserializer = additional_methods_for_deserialization[header]
    else:
        logger.info("Trying to dynamically load deserializer: {!r}".format(header))
        # This is a user plugin point, so expect exceptions to happen.
        try:
            module_name, class_name = header.split(b' ', 1)
            decoded_module_name = module_name.decode('utf-8')
            module = importlib.import_module(decoded_module_name)
            deserializer_class = getattr(module, class_name.decode('utf-8'))
            deserializer = deserializer_class()
            additional_methods_for_deserialization[header] = deserializer
        except Exception as e:
            raise DeserializerPluginError(header) from e

    result = deserializer.deserialize(body)

    return result


def pack_buffers(buffers: List[bytes]) -> bytes:
    """
    Parameters
    ----------
    buffers: list of byte strings
    """
    packed = b''
    for buf in buffers:
        s_length = bytes(str(len(buf)) + '\n', 'utf-8')
        packed += s_length + buf

    return packed


def unpack_buffers(packed_buffer: bytes) -> List[bytes]:
    """
    Parameters
    ----------
    packed_buffers : packed buffer as byte sequence
    """
    unpacked = []
    while packed_buffer:
        s_length, buf = packed_buffer.split(b'\n', 1)
        i_length = int(s_length.decode('utf-8'))
        current, packed_buffer = buf[:i_length], buf[i_length:]
        unpacked.extend([current])

    return unpacked


def unpack_and_deserialize(packed_buffer: bytes) -> Any:
    """ Unpacks a packed buffer of 3 byte sequences and returns the
    deserialized contents for use in function application.
    Parameters
    ----------
    packed_buffers : packed buffer of 3 byte sequences
    """
    unpacked = []
    while packed_buffer:
        s_length, buf = packed_buffer.split(b'\n', 1)
        i_length = int(s_length.decode('utf-8'))
        current, packed_buffer = buf[:i_length], buf[i_length:]
        deserialized = deserialize(current)
        unpacked.extend([deserialized])

    assert len(unpacked) == 3, "Unpack expects 3 buffers, got {}".format(len(unpacked))

    return unpacked
