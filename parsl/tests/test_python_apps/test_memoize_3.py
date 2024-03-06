import pytest
import enum

# Define an enum - collection of related consonants


class Foo(enum.Enum):
    x = enum.auto()
    y = enum.auto()


# Test function demonstrating the issue with unstable sorting when keys
# are hashable but not comparable.


def test_unstable_sorting():
    # Functions
    def foo():
        return 1

    def bar():
        return 2

    # Dictionary with problematic keys
    d = {foo: 1, bar: 2}

    # Sort the dictionary, it should raise a TypeError
    with pytest.raises(TypeError):
        sorted(d)

    # Create a dictionary with enum keys
    d_enum = {Foo.x: 1, Foo.y: 2}
