from parsl.serialize.base import SerializerBase
import serpent

from typing import Any


class SerpentSerializer(SerializerBase):
    _identifier = b'parsl.serialize.plugin_serpent SerpentSerializer'
    _for_code = False
    _for_data = True

    def serialize(self, data: Any) -> bytes:
        body = serpent.dumps(data)
        roundtripped_data = serpent.loads(body)

        # this round trip is because serpent will sometimes serialize objects
        # as best as it can, which is not good enough...
        if data != roundtripped_data:
            raise ValueError(f"SerpentSerializer cannot roundtrip {data} -> {body} -> {roundtripped_data}")
        return body

    def deserialize(self, body: bytes) -> Any:
        return serpent.loads(body)
