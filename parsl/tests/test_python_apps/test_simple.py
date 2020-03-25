from parsl.app.app import python_app


@python_app
def increment(x):
    return x + 1


@python_app
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x + 1


def test_increment(depth=5):
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = increment(futs[i - 1])

    x = sum([futs[i].result() for i in futs if not isinstance(futs[i], int)])
    assert x == sum(range(1, depth)), "[TEST] increment [FAILED]"


def test_slow_increment(depth=5):
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = slow_increment(futs[i - 1], 0.01)

    x = sum([futs[i].result() for i in futs if not isinstance(futs[i], int)])

    assert x == sum(range(1, depth)), "[TEST] slow_increment [FAILED]"
