from parsl.app.app import join_app, python_app


@join_app(cache=True)
def random_uuid(x):
    import uuid
    return const(str(uuid.uuid4()))


@python_app
def const(x):
    return x


def test_join_memoization():
    """Testing python memoization disable
    """
    x = random_uuid(0)
    x.result()

    for i in range(0, 2):
        foo = random_uuid(0)
        assert foo.result() == x.result(), "Memoized results were not used"
