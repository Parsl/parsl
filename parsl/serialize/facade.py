from parsl.serialize.concretes import *  # noqa: F403,F401
from parsl.serialize.base import METHODS_MAP_DATA, METHODS_MAP_CODE
import logging

logger = logging.getLogger(__name__)


class ParslSerializer(object):
    """ ParslSerializer gives you one interface to multiple serialization libraries
    """
    __singleton_instance = None

    def __new__(cls, *args, **kwargs):
        """ Make ParslSerializer a singleton using the fact that there's only one instance
        of a class variable
        """
        if ParslSerializer.__singleton_instance is None:
            ParslSerializer.__singleton_instance = object.__new__(cls, *args, **kwargs)
        return ParslSerializer.__singleton_instance

    def __init__(self):
        """ Instantiate the appropriate classes
        """
        headers = list(METHODS_MAP_CODE.keys()) + list(METHODS_MAP_DATA.keys())
        self.header_size = len(headers[0])

        self.methods_for_code = {}
        self.methods_for_data = {}

        for key in METHODS_MAP_CODE:
            self.methods_for_code[key] = METHODS_MAP_CODE[key]()
            self.methods_for_code[key].enable_caching(maxsize=128)

        for key in METHODS_MAP_DATA:
            self.methods_for_data[key] = METHODS_MAP_DATA[key]()

    def _list_methods(self):
        return self.methods_for_code, self.methods_for_data

    def pack_apply_message(self, func, args, kwargs, buffer_threshold=128 * 1e6):
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
        b_func = self.serialize(func, buffer_threshold=buffer_threshold)
        b_args = self.serialize(args, buffer_threshold=buffer_threshold)
        b_kwargs = self.serialize(kwargs, buffer_threshold=buffer_threshold)
        packed_buffer = self.pack_buffers([b_func, b_args, b_kwargs])
        return packed_buffer

    def unpack_apply_message(self, packed_buffer, user_ns=None, copy=False):
        """ Unpack and deserialize function and parameters

        """
        return [self.deserialize(buf) for buf in self.unpack_buffers(packed_buffer)]

    def serialize(self, obj, buffer_threshold=1e6):
        """ Try available serialization methods one at a time

        Individual serialization methods might raise a TypeError (eg. if objects are non serializable)
        This method will raise the exception from the last method that was tried, if all methods fail.
        """
        serialized = None
        serialized_flag = False
        last_exception = None
        if callable(obj):
            for method in self.methods_for_code.values():
                try:
                    serialized = method.serialize(obj)
                    # We attempt a deserialization to make sure both work.
                    method.deserialize(serialized)
                except Exception as e:
                    last_exception = e
                    continue
                else:
                    serialized_flag = True
                    break
        else:
            for method in self.methods_for_data.values():
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

    def deserialize(self, payload):
        """
        Parameters
        ----------
        payload : str
           Payload object to be deserialized

        """
        header = payload[0:self.header_size]
        if header in self.methods_for_code:
            result = self.methods_for_code[header].deserialize(payload)
        elif header in self.methods_for_data:
            result = self.methods_for_data[header].deserialize(payload)
        else:
            raise TypeError("Invalid header: {} in data payload. Buffer is either corrupt or not created by ParslSerializer".format(header))

        return result

    def pack_buffers(self, buffers):
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

    def unpack_buffers(self, packed_buffer):
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

    def unpack_and_deserialize(self, packed_buffer):
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
            deserialized = self.deserialize(current)
            unpacked.extend([deserialized])

        assert len(unpacked) == 3, "Unpack expects 3 buffers, got {}".format(len(unpacked))

        return unpacked
