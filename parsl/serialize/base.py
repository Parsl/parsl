from abc import abstractmethod
import logging
import functools

logger = logging.getLogger(__name__)

# GLOBALS
METHODS_MAP_CODE = {}
METHODS_MAP_DATA = {}


class SerializerBase(object):
    """ Adds shared functionality for all serializer implementations
    """

    def __init_subclass__(cls, *args, **kwargs):
        """ This forces all child classes to register themselves as
        methods for serializing code or data
        """
        super().__init_subclass__(*args, **kwargs)
        if cls._for_code:
            METHODS_MAP_CODE[cls._identifier] = cls
        if cls._for_data:
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

    def enable_caching(self, maxsize=128):
        """ Add functools.lru_cache onto the serialize, deserialize methods
        """

        self.serialize = functools.lru_cache(maxsize=maxsize)(self.serialize)
        self.deserialize = functools.lru_cache(maxsize=maxsize)(self.deserialize)
        return

    @abstractmethod
    def serialize(self, data):
        pass

    @abstractmethod
    def deserialize(self, payload):
        pass
