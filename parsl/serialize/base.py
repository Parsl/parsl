from abc import abstractmethod
import logging

from typing import Any

logger = logging.getLogger(__name__)

# GLOBALS
METHODS_MAP_CODE = {}
METHODS_MAP_DATA = {}


class SerializerBase:
    """ Adds shared functionality for all serializer implementations
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """ This forces all child classes to register themselves as
        methods for serializing code or data
        """
        super().__init_subclass__(**kwargs)

        if cls._for_code:
            METHODS_MAP_CODE[cls._identifier] = cls
        if cls._for_data:
            METHODS_MAP_DATA[cls._identifier] = cls

    _identifier: bytes
    _for_code: bool
    _for_data: bool

    @property
    def identifier(self) -> bytes:
        """Get that identifier that will be used to indicate in byte streams
        that this class should be used for deserialization.

        Returns
        -------
        identifier : bytes
        """
        return self._identifier

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, payload: bytes) -> Any:
        pass
