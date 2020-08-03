import dill
import pickle
import logging

logger = logging.getLogger(__name__)
from parsl.serialize.base import SerializerBase


class PickleSerializer(SerializerBase):
    """ Pickle serialization covers most python objects, with some notable exceptions:

    * functions defined in a interpretor/notebook
    * classes defined in local context and not importable using a fully qualified name
    * clojures, generators and coroutines
    * [sometimes] issues with wrapped/decorated functions
    """

    _identifier = b'01\n'
    _for_code = True
    _for_data = True

    def serialize(self, data):
        x = pickle.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(chomped)
        return data


class DillSerializer(SerializerBase):
    """ Dill serialization works on a superset of object including the ones covered by pickle.
    However for most cases pickle is faster. For most callable objects the additional overhead
    of dill can be amortized with an lru_cache. Here's items that dill handles that pickle
    doesn't:

    * functions defined in a interpretor/notebook
    * classes defined in local context and not importable using a fully qualified name
    * functions that are wrapped/decorated by other functions/classes
    * clojures
    """

    _identifier = b'02\n'
    _for_code = True
    _for_data = True

    def serialize(self, data):
        x = dill.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = dill.loads(chomped)
        return data
