from abc import abstractmethod
import logging
import functools

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
        """ Get the identifier of the serialization method

        Returns
        -------
        identifier : str
        """
        return self._identifier

    def enable_caching(self, maxsize: int = 128) -> None:
        """ Add functools.lru_cache onto the serialize, deserialize methods
        """

        # ignore types here because mypy at the moment is not fond of monkeypatching
        self.serialize = functools.lru_cache(maxsize=maxsize)(self.serialize)  # type: ignore[method-assign]
        self.deserialize = functools.lru_cache(maxsize=maxsize)(self.deserialize)  # type: ignore[method-assign]

        return

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, payload: bytes) -> Any:
        pass
