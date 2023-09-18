from parsl.app.app import python_app


@python_app
def app_double(x):
    return x * 2


@python_app
def app_sum(inputs=()):
    return sum(inputs)


def test_1(N=10):
    """Testing code snippet from the documentation
    """

    # Create a list of integers, then apply *app* function to each
    items = range(N)
    mapped_results = list(map(app_double, items))

    total = app_sum(inputs=mapped_results)
    assert total.result() == 2 * sum(items)
