import pytest

from parsl.serialize import pack_res_spec_apply_message, unpack_res_spec_apply_message


def double(x: int, y: int = 2) -> int:
    return x * y


@pytest.mark.local
def test_pack_and_unpack():
    args = (5,)
    kwargs = {'y': 10}
    resource_spec = {'num_nodes': 4}
    packed = pack_res_spec_apply_message(double, args, kwargs, resource_specification=resource_spec)

    unpacked = unpack_res_spec_apply_message(packed)
    assert len(unpacked) == 4
    u_fn, u_args, u_kwargs, u_res_spec = unpacked
    assert u_fn == double
    assert u_args == args
    assert u_kwargs == kwargs
    assert u_res_spec == resource_spec
