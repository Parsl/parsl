from parsl.app.app import python_app


@python_app
def double(x):
    raise ValueError("BENC deliberate fail")


def test_simple(n=1):
    x = double(n)
    x.exception()
