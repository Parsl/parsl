from parsl.serialize.base import SerializerBase

from typing import Any

import dill
import io
import logging
import sys
import types

logger = logging.getLogger(__name__)


def _deprotectCode(major: int, minor: int, b: bytes) -> Any:
    if sys.version_info.major != major:
        raise RuntimeError("Major version mismatch deserializing code")
    if sys.version_info.minor != minor:
        raise RuntimeError("Major version mismatch deserializing code")

    return dill.loads(b)


class CodeProtectorPickler(dill.Pickler):

    def reducer_override(self, o: Any) -> Any:
        logger.info(f"BENC: reducing object {o!r} of type {type(o)}")
        if isinstance(o, types.CodeType):
            logger.info(f"BENC: special casing code object {o!r} of type {type(o)}")
            return (_deprotectCode, (sys.version_info.major, sys.version_info.minor, dill.dumps(o)))

        return NotImplemented


class CodeProtectorSerializer(SerializerBase):

    _identifier = b'parsl.serialize.plugin_codeprotector CodeProtectorSerializer'

    _for_code = True
    _for_data = False

    def serialize(self, data: Any) -> bytes:

        f = io.BytesIO()
        pickler = CodeProtectorPickler(file=f)
        pickler.dump(data)
        return f.getvalue()

    def deserialize(self, body: bytes) -> Any:
        return dill.loads(body)
