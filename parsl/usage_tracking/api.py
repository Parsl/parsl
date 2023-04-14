# this file provides the API for components of parsl to identify their
# usage information.

from parsl.utils import RepresentationMixin

from functools import singledispatch
from typing import Any, List, Sequence

# for default objects, return the class name if it is in the parsl
# hierarchy, otherwise ignore. (a different choice here would be to
# always return the class name, even for classes outside of the
# hierarchy)
# TODO: the Any here should be the set of types we can easily turn into
# json?


@singledispatch
def get_parsl_usage(obj) -> List[Any]:
    t = type(obj)
    if t.__module__.startswith("parsl."):
        qualified_name = t.__module__ + "." + t.__name__
        return [qualified_name]
    else:
        return []


# any of the parsl classes which we expect to have representationstylemixin
# style introspection, we can use the same introspection style here, perhaps:
@get_parsl_usage.register
def _(obj: RepresentationMixin):
    t = type(obj)
    qualified_name = t.__module__ + "." + t.__name__

    me = [qualified_name]

    # what is this unwrapping? (in practice... typeguard?)
    init: Any = type(obj).__init__
    if hasattr(init, '__wrapped__'):
        init = init.__wrapped__

    import inspect

    argspec = inspect.getfullargspec(init)
    print(f"argspec = {argspec}")

    for arg in argspec.args[1:]:  # skip first arg, self
        arg_value = getattr(obj, arg)
        print(arg + ": " + str(type(arg_value)))
        d = get_parsl_usage(arg_value)
        print(f"d list = {d}")
        me += d

    return me


@get_parsl_usage.register(list)
@get_parsl_usage.register(tuple)
def _(obj: Sequence):
    result = []
    for v in obj:
        print(f"recursing list element: {v}")
        result += get_parsl_usage(v)
    return result
