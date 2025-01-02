from typing import Tuple


def convert_to_binary(x: int) -> Tuple[bool, ...]:
    """Convert a nonnegative integer into a binary

    Args:
        x: Number to be converted
    Returns:
        The binary number represented as list of booleans
    """
    if x < 0:
        raise ValueError('`x` must be nonnegative')
    bin_as_string = bin(x)
    return tuple(i == '1' for i in bin_as_string[2:])
