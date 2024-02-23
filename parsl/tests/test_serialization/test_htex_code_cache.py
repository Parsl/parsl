from typing import Any

import pytest

import parsl
from parsl.serialize.facade import methods_for_code
from parsl.tests.configs.htex_local import fresh_config as local_config


@parsl.python_app
def f(x):
    return x + 1


@pytest.mark.local
def test_caching() -> None:
    # for future serializer devs: if this is failing because you added another
    # code serializer, you'll also probably need to re-think what is being tested
    # about serialization caching here.
    assert len(methods_for_code) == 1

    serializer = methods_for_code[b'C2']

    # force type to Any here because a serializer method coming from
    # methods_for_code doesn't statically have any cache management
    # methods on itself such as cache_clear or cache_info.
    serialize_method: Any = serializer.serialize

    serialize_method.cache_clear()

    assert serialize_method.cache_info().hits == 0
    assert serialize_method.cache_info().misses == 0
    assert serialize_method.cache_info().currsize == 0

    assert f(7).result() == 8

    # the code serializer cache should now contain only a (probably wrapped) f ...
    assert serialize_method.cache_info().currsize == 1

    # ... which was not already in the cache.
    assert serialize_method.cache_info().misses == 1
    assert serialize_method.cache_info().hits == 0

    assert f(100).result() == 101

    # this time round, we should have got a single cache hit...
    assert serialize_method.cache_info().hits == 1
    assert serialize_method.cache_info().misses == 1
    assert serialize_method.cache_info().currsize == 1

    assert f(200).result() == 201

    # this time round, we should have got another single cache hit...
    assert serialize_method.cache_info().hits == 2
    assert serialize_method.cache_info().misses == 1
    assert serialize_method.cache_info().currsize == 1
