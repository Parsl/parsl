import parsl


@parsl.bash_app
def my_app(cache=7):
    assert type(cache) == int
    return "true"


def test_default_value():
    my_app().result()


def test_specified_value():
    my_app(cache=8).result()
