from parsl.app.app import join_app, python_app


@python_app
def add(*args):
    """Add all of the arguments together. If no arguments, then
    zero is returned (the neutral element of +)
    """
    accumulator = 0
    for v in args:
        accumulator += v
    return accumulator


@join_app
def fibonacci(n):
    if n == 0:
        return add()
    elif n == 1:
        return add(1)
    else:
        return add(fibonacci(n - 1), fibonacci(n - 2))


def test_fibonacci():
    assert fibonacci(0).result() == 0
    assert fibonacci(4).result() == 3
    assert fibonacci(6).result() == 8
