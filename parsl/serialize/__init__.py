from parsl.serialize.facade import ParslSerializer
parsl_serializer = ParslSerializer()
serialize = parsl_serializer.serialize
deserialize = parsl_serializer.deserialize
pack_apply_message = parsl_serializer.pack_apply_message
unpack_apply_message = parsl_serializer.unpack_apply_message

__all__ = ['ParslSerializer',
           'serialize',
           'deserialize',
           'pack_apply_message',
           'unpack_apply_message']
