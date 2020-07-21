import codecs
import json
import dill
import pickle
import inspect
import logging

logger = logging.getLogger(__name__)
from parsl.serialize.base import fxPicker_shared


class json_base64(fxPicker_shared):

    _identifier = '00\n'
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = json.dumps(data)
        return self.identifier + x

    def deserialize(self, payload):
        x = json.loads(self.chomp(payload))
        return x


class pickle_base64(fxPicker_shared):

    _identifier = '01\n'
    _for_code = False

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        return data


class code_pickle(fxPicker_shared):

    _identifier = '02\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        x = codecs.encode(pickle.dumps(data), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        data = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        return data


class code_text_dill(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = '03\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = dill.source.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        exec(body)
        return locals()[name]


class code_text_inspect(fxPicker_shared):
    """ We use dill to get the source code out of the function object
    and then exec the function body to load it in. The function object
    is then returned by name.
    """

    _identifier = '04\n'
    _for_code = True

    def __init__(self):
        super().__init__()

    def serialize(self, data):
        name = data.__name__
        body = inspect.getsource(data)
        x = codecs.encode(pickle.dumps((name, body)), 'base64').decode()
        return self.identifier + x

    def deserialize(self, payload):
        chomped = self.chomp(payload)
        name, body = pickle.loads(codecs.decode(chomped.encode(), 'base64'))
        exec(body)
        return locals()[name]
