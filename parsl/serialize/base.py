from abc import abstractmethod
import logging

from typing import Any

logger = logging.getLogger(__name__)


class SerializerBase:
    """ Adds shared functionality for all serializer implementations
    """

    _identifier: bytes

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
