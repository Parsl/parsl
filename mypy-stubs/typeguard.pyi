
# this type for runtime_validation is based on the
# "Decorators that do not change the signature of the function" section of
# https://github.com/python/mypy/issues/3157

from typing import TypeVar

Func = TypeVar('Func')

def typechecked(f: Func) -> Func: ...
