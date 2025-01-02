"""Functions used as part of the workflow"""
from typing import List, Tuple

from .logic import convert_to_binary


def convert_many_to_binary(xs: List[int]) -> List[Tuple[bool, ...]]:
    """Convert a list of nonnegative integers to binary"""
    return [convert_to_binary(x) for x in xs]
