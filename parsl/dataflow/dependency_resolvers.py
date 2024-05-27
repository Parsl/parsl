from concurrent.futures import Future
from dataclasses import dataclass
from functools import singledispatch
from typing import Callable, Sequence


@dataclass
class DependencyResolver:
    """A DependencyResolver describes how app dependencies can be resolved.
    It is specified as two functions: `traverse_to_gather` which turns an
    app parameter into a sequence of futures which must be waited for before
    the task can be executed (for example, in the case of
    `DEEP_DEPENDENCY_RESOLVER` this traverses structures such as lists to
    find every contained ``Future``), and `traverse_to_unwrap` which turns an
    app parameter into its value to be passed to the app on execution
    (for example in the case of `DEEP_DEPENDENCY_RESOLVER` this replaces a
    list containing futures with a new list containing the values of those
    resolved futures).

    By default, Parsl will use `SHALLOW_DEPENDENCY_RESOLVER` which only
    resolves Futures passed directly as arguments.
    """
    traverse_to_gather: Callable[[object], Sequence[Future]]
    traverse_to_unwrap: Callable[[object], object]


@singledispatch
def shallow_traverse_to_gather(o):
    # objects in general do not expose futures that we can see
    return []


@singledispatch
def shallow_traverse_to_unwrap(o):
    # objects in general unwrap to themselves
    return o


@shallow_traverse_to_gather.register
def _(fut: Future):
    return [fut]


@shallow_traverse_to_unwrap.register
@singledispatch
def _(fut: Future):
    assert fut.done()
    return fut.result()


@singledispatch
def deep_traverse_to_gather(o):
    # objects in general do not expose futures that we can see
    return []


@singledispatch
def deep_traverse_to_unwrap(o):
    # objects in general unwrap to themselves
    return o


@deep_traverse_to_gather.register
def _(fut: Future):
    return [fut]


@deep_traverse_to_unwrap.register
@singledispatch
def _(fut: Future):
    assert fut.done()
    return fut.result()


@deep_traverse_to_gather.register(tuple)
@deep_traverse_to_gather.register(list)
@deep_traverse_to_gather.register(set)
def _(iterable):
    return [e for v in iterable for e in deep_traverse_to_gather(v)]


@deep_traverse_to_unwrap.register(tuple)
@deep_traverse_to_unwrap.register(list)
@deep_traverse_to_unwrap.register(set)
@singledispatch
def _(iterable):

    type_ = type(iterable)
    return type_(map(deep_traverse_to_unwrap, iterable))


@deep_traverse_to_gather.register(dict)
def _(dictionary):
    futures = []
    for key, value in dictionary.items():
        futures.extend(deep_traverse_to_gather(key))
        futures.extend(deep_traverse_to_gather(value))
    return futures


@deep_traverse_to_unwrap.register(dict)
def _(dictionary):
    unwrapped_dict = {}
    for key, value in dictionary.items():
        key = deep_traverse_to_unwrap(key)
        value = deep_traverse_to_unwrap(value)
        unwrapped_dict[key] = value
    return unwrapped_dict


DEEP_DEPENDENCY_RESOLVER = DependencyResolver(traverse_to_gather=deep_traverse_to_gather,
                                              traverse_to_unwrap=deep_traverse_to_unwrap)

SHALLOW_DEPENDENCY_RESOLVER = DependencyResolver(traverse_to_gather=shallow_traverse_to_gather,
                                                 traverse_to_unwrap=shallow_traverse_to_unwrap)
