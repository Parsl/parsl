from __future__ import annotations

import typing as t
from typing import Protocol, TypeVar

import importlib

import parsl.serialize.serializer
from parsl.serialize.facade import (
    deserialize,
    pack_apply_message,
    serialize,
    unpack_apply_message,
)


class SerializerLoadError(Exception):
    ...


@t.runtime_checkable
class SerializerProtocol(Protocol):

    def serialize_task(self, func: t.Callable, args: t.Sequence, kwargs: t.Dict) -> bytes:
        ...

    def deserialize_task(self, task_buffer: bytes) -> t.Tuple[t.Callable, t.Sequence, t.Dict]:
        ...

    def serialize_result(self, py_obj: t.Any) -> bytes:
        ...

    def deserialize_result(self, buffer: bytes) -> t.Any:
        ...


SerializerT = TypeVar('SerializerT', bound=SerializerProtocol)


class Serializer:

    def __init__(self, buffer_threshold: int = int(1e6)):
        self.buffer_threshold = buffer_threshold

    def serialize_task(self, func: t.Callable, args: t.Sequence, kwargs: t.Dict) -> bytes:
        return pack_apply_message(func, args, kwargs, buffer_threshold=self.buffer_threshold)

    def deserialize_task(self, task_message: str) -> t.Tuple[t.Callable, t.Sequence, t.Dict]:
        return unpack_apply_message(task_message)  #type: ignore[arg-type, return-value]

    def serialize_result(self, py_obj: t.Any) -> bytes:
        return serialize(py_obj)

    def deserialize_result(self, message: bytes) -> t.Any:
        return deserialize(message)



def safe_load(module_path, class_name) -> t.Type[SerializerT]:

    try:
        module = importlib.import_module(module_path)

        cls = getattr(module, class_name, None)
        if cls is None:
            raise ImportError(
            f"Class '{class_name}' not found in module '{module_path}'.")
        return cls
    except ImportError:
        raise ImportError(f"Module '{module_name}' could not be imported.")




def load_serializer(serializer_mod_path: str) -> SerializerT:
    """Loads a serializer from the serializer_mod_path which must
    match the form: path.to.module.SerializerClass(args) """
    import importlib
    try:
        mod_info, _sep, _args = serializer_mod_path.partition('(')
        module_path, _sep, class_name = mod_info.rpartition('.')

        importlib.import_module(serializer_mod_path)
    except Exception as e:
        raise SerializerLoadError(f"Serializer failed to load from: {serializer_mod_path}")
