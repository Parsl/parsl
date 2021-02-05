from parsl.app.app import join_app, python_app


@python_app
def sum(*args):
    accumulator = 0
    for v in args:
        accumulator += v
    return accumulator


@join_app
def fibonacci(n):
    if n == 0:
        return sum()
    elif n == 1:
        return sum(1)
    else:
        return sum(fibonacci(n - 1), fibonacci(n - 2))


def test_fibonacci():
    assert fibonacci(0).result() == 0
    assert fibonacci(4).result() == 3
    assert fibonacci(10).result() == 55
