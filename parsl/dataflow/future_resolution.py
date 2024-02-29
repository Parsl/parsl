from concurrent.futures import Future
from functools import singledispatch

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

