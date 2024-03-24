from concurrent.futures import Future
from functools import singledispatch
from collections.abc import Mapping


@singledispatch
def traverse_to_gather(o):
    # objects in general do not expose futures that we can see
    return []


@singledispatch
def traverse_to_unwrap(o):
    # objects in general unwrap to themselves
    return o


@traverse_to_gather.register
def _(fut: Future):
    return [fut]


@traverse_to_unwrap.register
@singledispatch
def _(fut: Future):
    return fut.result()


# The above is the traditional Parsl future/non-future behaviour.
# Below is an example of shallow traversal of iterables.


@traverse_to_gather.register(tuple)
@traverse_to_gather.register(list)
@traverse_to_gather.register(set)
def _(iterable):
    # a "deep" traversal would instead recursively call traverse_to_gather
    # here to inspect whatever is inside the sequence

    return [v for v in iterable if isinstance(v, Future)]


@traverse_to_unwrap.register(tuple)
@traverse_to_unwrap.register(list)
@traverse_to_unwrap.register(set)
@singledispatch
def _(iterable):
    def unwrap(v):
        if isinstance(v, Future):
            assert (
                v.done()
            ), "sequencing error: v should be done by now, otherwise weird hangs in DFK"
            return v.result()
        else:
            return v

    type_ = type(iterable)
    return type_(map(unwrap, iterable))


@traverse_to_gather.register(Mapping)
def _(dictionary):
    futures = []
    for key, value in dictionary.items():
        if isinstance(key, Future):
            futures.append(key)
        if isinstance(value, Future):
            futures.append(value)
    return futures


@traverse_to_unwrap.register(Mapping)
def _(dictionary):
    unwrapped_dict = {}
    for key, value in dictionary.items():
        if isinstance(key, Future):
            assert key.done(), "key future should be done by now"
            key = key.result()
        if isinstance(value, Future):
            assert value.done(), "value future should be done by now"
            value = value.result()
        unwrapped_dict[key] = value
    return unwrapped_dict
