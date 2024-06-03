import functools
import logging
import pickle

import dill

logger = logging.getLogger(__name__)
from typing import Any

from parsl.serialize.base import SerializerBase


class PickleSerializer(SerializerBase):
    """ Pickle serialization covers most python objects, with some notable exceptions:

    * functions defined in a interpreter/notebook
    * classes defined in local context and not importable using a fully qualified name
    * closures, generators and coroutines
    """

    identifier = b'01'

    def serialize(self, data: Any) -> bytes:
        return pickle.dumps(data)

    def deserialize(self, body: bytes) -> Any:
        return pickle.loads(body)


class DillSerializer(SerializerBase):
    """ Dill serialization works on a superset of object including the ones covered by pickle.
    However for most cases pickle is faster. For most callable objects the additional overhead
    of dill can be amortized with an lru_cache. Here's items that dill handles that pickle
    doesn't:

    * functions defined in a interpreter/notebook
    * classes defined in local context and not importable using a fully qualified name
    * functions that are wrapped/decorated by other functions/classes
    * closures
    """

    identifier = b'02'

    def serialize(self, data: Any) -> bytes:
        return dill.dumps(data)

    def deserialize(self, body: bytes) -> Any:
        return dill.loads(body)


class DillCallableSerializer(SerializerBase):
    """This serializer is a variant of the DillSerializer that will
    serialize and deserialize callables using an lru_cache, under the
    assumption that callables are immutable and so can be cached.
    """

    identifier = b'C2'

    @functools.lru_cache
    def serialize(self, data: Any) -> bytes:
        return dill.dumps(data)

    @functools.lru_cache
    def deserialize(self, body: bytes) -> Any:
        return dill.loads(body)
