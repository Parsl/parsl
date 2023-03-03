from parsl.serialize.concretes import *  # noqa: F403,F401
from parsl.serialize.base import METHODS_MAP_DATA, METHODS_MAP_CODE
import logging

logger = logging.getLogger(__name__)


""" Instantiate the appropriate classes
"""
headers = list(METHODS_MAP_CODE.keys()) + list(METHODS_MAP_DATA.keys())
header_size = len(headers[0])

methods_for_code = {}
methods_for_data = {}

for key in METHODS_MAP_CODE:
    methods_for_code[key] = METHODS_MAP_CODE[key]()
    methods_for_code[key].enable_caching(maxsize=128)

    for key in METHODS_MAP_DATA:
        methods_for_data[key] = METHODS_MAP_DATA[key]()


def _list_methods():
    return methods_for_code, methods_for_data


def pack_apply_message(func, args, kwargs, buffer_threshold=128 * 1e6):
    """Serialize and pack function and parameters

    Parameters
    ----------

    func: Function
        A function to ship

    args: Tuple/list of objects
        positional parameters as a list

    kwargs: Dict
        Dict containing named parameters

    buffer_threshold: Ignored
        Limits buffer to specified size in bytes. Exceeding this limit would give you
        a warning in the log. Default is 128MB.
    """
    b_func = serialize(func, buffer_threshold=buffer_threshold)
    b_args = serialize(args, buffer_threshold=buffer_threshold)
    b_kwargs = serialize(kwargs, buffer_threshold=buffer_threshold)
    packed_buffer = pack_buffers([b_func, b_args, b_kwargs])
    return packed_buffer


def unpack_apply_message(packed_buffer, user_ns=None, copy=False):
    """ Unpack and deserialize function and parameters

    """
    return [deserialize(buf) for buf in unpack_buffers(packed_buffer)]


def serialize(obj, buffer_threshold=1e6):
    """ Try available serialization methods one at a time

    Individual serialization methods might raise a TypeError (eg. if objects are non serializable)
    This method will raise the exception from the last method that was tried, if all methods fail.
    """
    serialized = None
    serialized_flag = False
    last_exception = None
    if callable(obj):
        for method in methods_for_code.values():
            try:
                serialized = method.serialize(obj)
            except Exception as e:
                last_exception = e
                continue
            else:
                serialized_flag = True
                break
    else:
        for method in methods_for_data.values():
            try:
                serialized = method.serialize(obj)
            except Exception as e:
                last_exception = e
                continue
            else:
                serialized_flag = True
                break

    if serialized_flag is False:
        # TODO : Replace with a SerializationError
        raise last_exception

    if len(serialized) > buffer_threshold:
        logger.warning(f"Serialized object exceeds buffer threshold of {buffer_threshold} bytes, this could cause overflows")
    return serialized


def deserialize(payload):
    """
    Parameters
    ----------
    payload : str
       Payload object to be deserialized

    """
    header = payload[0:header_size]
    if header in methods_for_code:
        result = methods_for_code[header].deserialize(payload)
    elif header in methods_for_data:
        result = methods_for_data[header].deserialize(payload)
    else:
        raise TypeError("Invalid header: {} in data payload. Buffer is either corrupt or not created by ParslSerializer".format(header))

    return result


def pack_buffers(buffers):
    """
    Parameters
    ----------
    buffers : list of \n terminated strings
    """
    packed = b''
    for buf in buffers:
        s_length = bytes(str(len(buf)) + '\n', 'utf-8')
        packed += s_length + buf

    return packed


def unpack_buffers(packed_buffer):
    """
    Parameters
    ----------
    packed_buffers : packed buffer as string
    """
    unpacked = []
    while packed_buffer:
        s_length, buf = packed_buffer.split(b'\n', 1)
        i_length = int(s_length.decode('utf-8'))
        current, packed_buffer = buf[:i_length], buf[i_length:]
        unpacked.extend([current])

    return unpacked


def unpack_and_deserialize(packed_buffer):
    """ Unpacks a packed buffer and returns the deserialized contents
    Parameters
    ----------
    packed_buffers : packed buffer as string
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
