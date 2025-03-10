from parsl.serialize.facade import (
    deserialize,
    pack_apply_message,
    pack_res_spec_apply_message,
    serialize,
    unpack_apply_message,
    unpack_res_spec_apply_message,
)

serialize_task = pack_res_spec_apply_message
deserialize_task = unpack_res_spec_apply_message
serialize_result = serialize
deserialize_result = deserialize


__all__ = ['serialize',
           'deserialize',
           'pack_apply_message',
           'unpack_apply_message',
           'unpack_res_spec_apply_message',
           'pack_res_spec_apply_message',
           'serialize_task',
           'serialize_result',
           'deserialize_task',
           'deserialize_result'
           ]
