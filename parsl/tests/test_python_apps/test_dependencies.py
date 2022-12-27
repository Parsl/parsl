import parsl


@parsl.python_app
def k(x):
    return x


@parsl.python_app
def mul(left, right):
    return left * right


def test_diamond_dag():
    a = k(2)
    b = mul(a, 3)
    c = mul(a, 5)
    d = mul(b, c)
    e = k(d)

    assert e.result() == (2 * 3) * (2 * 5)
