import json
import dill
import pickle
import logging
import functools
import inspect

logger = logging.getLogger(__name__)
from parsl.serialize.base import fxPicker_shared


class pickle_base64(fxPicker_shared):

    _identifier = b'10\n'
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = pickle.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(chomped)
        return data


class dill_base64(fxPicker_shared):

    _identifier = b'11\n'
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = dill.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = dill.loads(chomped)
        return data


class json_base64(fxPicker_shared):
    """ Json based serialization does not guarantee consistency on tuple/list types
    Should be lower priority that pickle/dill
    """

    _identifier = '12\n'
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = json.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        x = json.loads(self.chomp(payload))
        return x


class code_pickle(fxPicker_shared):

    _identifier = b'00\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    @functools.lru_cache(maxsize=100)
    def serialize(self, data):
        return self.identifier + pickle.dumps(data)

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(chomped)
        return data


class code_dill(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = b'01\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    @functools.lru_cache(maxsize=100)
    def serialize(self, data):
        x = dill.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        fn = dill.loads(chomped)
        return fn


class code_text_inspect(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = b'02\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    @functools.lru_cache(maxsize=100)
    def serialize(self, data):
        name = data.__name__
        body = inspect.getsource(data)
        x = pickle.dumps((name, body))
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(chomped)
        exec(body)
        return locals()[name]


class code_text_dill(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = b'03\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    @functools.lru_cache(maxsize=100)
    def serialize(self, data):
        name = data.__name__
        body = dill.source.getsource(data)
        x = pickle.dumps((name, body))
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(chomped)
        exec(body)
        return locals()[name]
