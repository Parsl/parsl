import json
import dill
import pickle
import logging

logger = logging.getLogger(__name__)
from parsl.serialize.base import fxPicker_shared


class PickleSerializer(fxPicker_shared):
    """ Pickle serialization covers most python objects, with some notable exceptions:

    * functions defined in a interpretor/notebook
    * classes defined in local context and not importable using a fully qualified name
    * clojures, generators and coroutines
    * [sometimes] issues with wrapped/decorated functions
    """

    _identifier = b'01\n'
    _for_code = True
    _for_data = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = pickle.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(chomped)
        return data


class DillSerializer(fxPicker_shared):
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

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = dill.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = dill.loads(chomped)
        return data


class JsonSerializer(fxPicker_shared):
    """ Json based serialization does not guarantee consistency on tuple/list types
    Should be lower priority that pickle/dill. This is here for legacy reasons.
    In the funcX + automate case the automate service requires json serialization,
    and cannot handle pickle/dill.

    Unlike pickle and dill, json is a string based serializer. For consistency
    this class will do utf-8 encoding of the json-serialized strings to bytes for
    consistency
    """

    _identifier = b'03\n'
    _for_code = False
    _for_data = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = json.dumps(data).encode('utf-8')
        return self.identifier + x

    def deserialize(self, payload):
        x = json.loads(self.chomp(payload).decode('utf-8'))
        return x
