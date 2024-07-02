# test the serializer plugin API

import pytest

from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import (
    deserialize,
    register_method_for_data,
    serialize,
    unregister_serializer,
)

B_MAGIC = b'3626874628368432'  # arbitrary const bytestring
V_MAGIC = 777  # arbitrary const object


class ConstSerializer(SerializerBase):
    """This is a test deliberately misbehaving serializer.
    It serializes all values to the same constant, and likewise for
    deserialization.
    """

    # TODO: better enforcement of the presence of these values? tied into abstract base class?

    def serialize(self, o):
        return B_MAGIC

    def deserialize(self, b):
        assert b == B_MAGIC
        return V_MAGIC

# ugh... registering in globus state which will screw with other tests
# so should protect with a finally block:
# serializer registration is, generally, intended to be a not-undoable
# operation, though...


@pytest.mark.local
def test_const_inprocess():
    s = ConstSerializer()
    register_method_for_data(s)

    try:

        # check that we aren't using one of the real serializers
        # that really works
        assert deserialize(serialize(1)) != 1

        # and that the behaviour looks like ConstSerializer
        assert serialize(1) == s.identifier + b'\n' + B_MAGIC
        assert deserialize(serialize(1)) == V_MAGIC
    finally:
        unregister_serializer(s)

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
