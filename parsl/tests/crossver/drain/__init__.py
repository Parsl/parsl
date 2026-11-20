from ..util import Combo, interoperable


def compatible(combo: Combo, oldest: str) -> bool:
    return interoperable(combo, oldest)
