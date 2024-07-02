from typing import Any

import serpent

from parsl.serialize.base import SerializerBase


class SerpentSerializer(SerializerBase):

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
