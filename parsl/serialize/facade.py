import importlib
import logging
from typing import Any, Dict, List, Union

import parsl.serialize.concretes as concretes
from parsl.serialize.base import SerializerBase
from parsl.serialize.errors import DeserializerPluginError

logger = logging.getLogger(__name__)

methods_for_code: List[SerializerBase]
methods_for_code = []

methods_for_data: List[SerializerBase]
methods_for_data = []

deserializers: Dict[bytes, SerializerBase]
deserializers = {}


# maybe it's weird to want to clear away all serializers rather than just the
# data or just code ones? eg in proxystore test, clearing serializers means
# there is no code serializer... so just data sreializer should be cleared
# (or if only having one kind of serializer, no code/data distinction, then
# this is more consistent, but clearing serializer is still a bit weird in
#  that case (maybe for security?) - with inserting one near the start more
# usual?
# def clear_serializers() -> None:
#    # does not clear deserializers because remote sending back will have a
#    # different serializer list (and wants to send back results with one of
#    # the default 4) so clearing all the deserializers means we cannot receive
#    # results...
#    global methods_for_data, methods_for_code
#    methods_for_code = []
#    methods_for_data = []


def unregister_serializer(serializer: SerializerBase) -> None:
    logger.info(f"BENC: deserializers {deserializers}, serializer {serializer}")
    logger.info(f"BENC: unregistering serializer {serializer}")
    if serializer.identifier in deserializers:
        del deserializers[serializer.identifier]
    else:
        logger.warning("BENC: not found in deserializers list")
    if serializer in methods_for_code:
        logger.info("BENC: removing serializer from methods_for_code")
        methods_for_code.remove(serializer)
    else:
        logger.warning("BENC: not found in methods for code")
    if serializer in methods_for_data:
        logger.info("BENC: removing serializer from methods_for_data")
        methods_for_data.remove(serializer)
    else:
        logger.warning("BENC: not found in methods for data")


# structuring it this way is probably wrong - should perhaps be a single
# Pickle-variant (or dill-variant) that is used, with all pluggable hooked
# in - eg proxystore should hook into the same Pickle/Dill subclass as
# CodeProtector?

def register_method_for_code(s: SerializerBase) -> None:
    deserializers[s.identifier] = s
    methods_for_code.insert(0, s)


register_method_for_code(concretes.DillCallableSerializer())
# register_method_for_code(CodeProtectorSerializer())


def register_method_for_data(s: SerializerBase) -> None:
    deserializers[s.identifier] = s
    methods_for_data.insert(0, s)


# These must be registered in reverse order of
# importance: later registered serializers
# will take priority over earlier ones. This is
# to facilitate user registered serializers

register_method_for_data(concretes.PickleSerializer())
register_method_for_data(concretes.DillSerializer())


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
    logger.info(f"BENC: Trying to serialize {obj}")
    result: Union[bytes, Exception, None]
    result = None
    if callable(obj):
        methods = methods_for_code
    else:
        methods = methods_for_data

    if methods == []:
        raise RuntimeError("There are no configured serializers")

    for method in methods:
        try:
            logger.info(f"BENC: trying serializer {method}")
            result = method.identifier + b'\n' + method.serialize(obj)
        except Exception as e:
            logger.warning(f"BENC: serializer {method} skipping, with exception: {e}")
            result = e
            continue
        else:
            break

    if result is None:
        raise RuntimeError("No serializer returned a result")
    elif isinstance(result, BaseException):
        logger.error("Serializer returned an excepton, reraise")
        raise result
    else:
        logger.debug("Serialization complete")
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

    if header in deserializers:
        deserializer = deserializers[header]
    else:
        logger.info("Trying to dynamically load deserializer: {!r}".format(header))
        # This is a user plugin point, so expect exceptions to happen.
        try:
            module_name, class_name = header.split(b' ', 1)
            decoded_module_name = module_name.decode('utf-8')
            module = importlib.import_module(decoded_module_name)
            deserializer_class = getattr(module, class_name.decode('utf-8'))
            deserializer = deserializer_class()
            deserializers[header] = deserializer
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
