from abc import ABCMeta, abstractmethod
import logging

logger = logging.getLogger(__name__)

# GLOBALS
METHODS_MAP_CODE = {}
METHODS_MAP_DATA = {}


class DeserializationError(Exception):
    """ Base class for all deserialization errors
    """
    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Deserialization failed due to {}".format(self.reason)

    def __str__(self):
        return self.__repr__()


class fxPicker_enforcer(metaclass=ABCMeta):
    """ Ensure that any concrete class will have the serialize and deserialize methods
    """

    @abstractmethod
    def serialize(self, data):
        pass

    @abstractmethod
    def deserialize(self, payload):
        pass


class fxPicker_shared(object):
    """ Adds shared functionality for all serializer implementations
    """

    def __init_subclass__(cls, *args, **kwargs):
        """ This forces all child classes to register themselves as
        methods for serializing code or data
        """
        super().__init_subclass__(*args, **kwargs)
        if cls._for_code:
            METHODS_MAP_CODE[cls._identifier] = cls
        else:
            METHODS_MAP_DATA[cls._identifier] = cls

    @property
    def identifier(self):
        """ Get the identifier of the serialization method

        Returns
        -------
        identifier : str
        """
        return self._identifier

    def chomp(self, payload):
        """ If the payload starts with the identifier, return the remaining block

        Parameters
        ----------
        payload : str
            Payload blob
        """
        s_id, payload = payload.split(b'\n', 1)
        if (s_id + b'\n') != self.identifier:
            raise TypeError("Buffer does not start with parsl.serialize identifier:{}".format(self.identifier))
        return payload
