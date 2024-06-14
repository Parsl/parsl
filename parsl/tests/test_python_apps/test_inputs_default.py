from parsl import python_app


@python_app
def identity(inp):
    return inp


@python_app
def add_inputs(inputs=[identity(1), identity(2)]):
    return sum(inputs)


def test_default_inputs():
    assert add_inputs().result() == 3
