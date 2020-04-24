from parsl.app.app import python_app


@python_app(cache=True, ignore_for_cache=[])
def random_uuid(x):
    import uuid
    return str(uuid.uuid4())


@python_app(cache=True, ignore_for_cache=['x'])
def random_uuid_def(x):
    import uuid
    return str(uuid.uuid4())


def test_memo_different():
    # explicitly call result() before launching next app so that
    # the executions are serialized and the x result will be in
    # memo table
    x = random_uuid(x=0).result()
    y = random_uuid(x=1).result()

    assert x != y, "Memoized results were used incorrectly"


def test_memo_same_at_definition():
    x = random_uuid_def(x=0).result()
    y = random_uuid_def(x=1).result()

    assert x == y, "Memoized results were not used"
