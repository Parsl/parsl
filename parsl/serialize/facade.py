from parsl.serialize.concretes import *  # noqa: F403,F401
from parsl.serialize.base import METHODS_MAP_DATA, METHODS_MAP_CODE, SerializerBase
import logging

from typing import Any, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)


""" Instantiate the appropriate classes
"""
methods_for_code = {}
methods_for_data = {}

for key in METHODS_MAP_CODE:
    methods_for_code[key] = METHODS_MAP_CODE[key]()
    methods_for_code[key].enable_caching(maxsize=128)

for key in METHODS_MAP_DATA:
    methods_for_data[key] = METHODS_MAP_DATA[key]()


def _list_methods() -> Tuple[Dict[bytes, SerializerBase], Dict[bytes, SerializerBase]]:
    return methods_for_code, methods_for_data


def pack_apply_message(func: Any, args: Any, kwargs: Any, buffer_threshold: int = int(128 * 1e6)) -> bytes:
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
    b_func = serialize(func, buffer_threshold=buffer_threshold)
    b_args = serialize(args, buffer_threshold=buffer_threshold)
    b_kwargs = serialize(kwargs, buffer_threshold=buffer_threshold)
    packed_buffer = pack_buffers([b_func, b_args, b_kwargs])
    return packed_buffer


def unpack_apply_message(packed_buffer: bytes, user_ns: Any = None, copy: Any = False) -> List[Any]:
    """ Unpack and deserialize function and parameters

    """
    return [deserialize(buf) for buf in unpack_buffers(packed_buffer)]


def serialize(obj: Any, buffer_threshold: int = int(1e6)) -> bytes:
    """ Try available serialization methods one at a time

    Individual serialization methods might raise a TypeError (eg. if objects are non serializable)
    This method will raise the exception from the last method that was tried, if all methods fail.
    """
    result: Union[bytes, Exception]
    if callable(obj):
        for method in methods_for_code.values():
            try:
                result = method._identifier + b'\n' + method.serialize(obj)
            except Exception as e:
                result = e
                continue
            else:
                break
    else:
        for method in methods_for_data.values():
            try:
                result = method._identifier + b'\n' + method.serialize(obj)
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
        result = methods_for_code[header].deserialize(body)
    elif header in methods_for_data:
        result = methods_for_data[header].deserialize(body)
    else:
        raise TypeError("Invalid header: {!r} in data payload. Buffer is either corrupt or not created by ParslSerializer".format(header))

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
