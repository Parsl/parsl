import codecs
import importlib
import json
import typing as t

from parsl.serialize.serializer import SerializerT

DEFAULT_SERIALIZER: str = 'parsl.serialize.serializer.Serializer'


class JsonSerializable:

    def __setstate__(self, state: str) -> None:
        self.__dict__.update(json.loads(state))

    def __getstate__(self) -> str:
        return json.dumps(self.__dict__)


class JsonSlotSerializable:

    # This is a placeholder and concretes must set it
    __slots__: t.List[str] = []

    def __getstate__(self) -> str:
        state = {slot: getattr(self, slot) for slot in self.__slots__}
        return json.dumps(state)

    def __setstate__(self, state: str) -> None:
        data = json.loads(state)
        for slot in self.__slots__:
            setattr(self, slot, data.get(slot))


class TaskMessage(JsonSlotSerializable):

    __slots__ = ['task_id',
                 'task_buffer',
                 'resource_specification',
                 'serializer_mod_info']

    def __init__(self, task_id: t.Union[int, str],
                 task_buffer: bytes,
                 resource_specification: t.Dict[str, t.Any],
                 serializer_mod_info: str = DEFAULT_SERIALIZER,
                 ):
        self.task_id = task_id
        self.resource_specification = resource_specification
        self.serializer_mod_info = serializer_mod_info
        self.task_buffer: str = ''
        self.store_inner(task_buffer)

    def store_inner(self, buffer: bytes):
        self.task_buffer = codecs.encode(buffer, "base64").decode()

    def get_inner(self) -> bytes:
        return codecs.decode(self.task_buffer, "base64")

    def _parse_mod_info(self, mod_info: str) -> t.Tuple[str, str, str]:
        mod_info, _sep, serializer_kwargs = mod_info.rpartition(':')
        module_path, _sep, obj_name = mod_info.rpartition('.')
        return module_path, obj_name, serializer_kwargs


    def resolve_serializer(self) -> SerializerT:
        """Note: This function is expected to run on the worker context which may not match
        executor env, therefore validation at message creation is not useful"""
        module_path, obj_name, serializer_kwargs = self._parse_mod_info(self.serializer_mod_info)

        module = importlib.import_module(module_path)
        serializer_cls = getattr(module, obj_name)

        # Instantiating a class from an arbitrary string is a potential
        # vulnerability, similar to concerns around serializers in general
        json.load(serializer_kwargs)
        serializer_kwargs = serializer_kwargs if serializer_kwargs else '{}'
        kwargs = json.loads(serializer_kwargs)
        self.serializer = serializer_cls(**kwargs)
        return self.serializer



if __name__ == "__main__":

    t = TaskMessage(task_id='task_id', task_buffer=b'bytes',
                    resource_specification={'num_nodes': 5})

    import pickle

    serialized = pickle.dumps(t)
    print(serialized)
    t2 = pickle.loads(serialized)
    assert t.task_id == t2.task_id

