import parsl


@parsl.python_app
def k(x):
    return x


@parsl.python_app
def mul(left, right):
    return left * right


def test_diamond_dag(caplog):
    a = k(2)
    b = mul(a, 3)
    c = mul(a, 5)
    d = mul(b, c)
    e = k(d)

    assert e.result() == (2 * 3) * (2 * 5)

    dep_pairs = [(e.__dict__["parsl.task"], e.__dict__["parsl.dependency_task"])
                 for e in caplog.records
                 if hasattr(e, "parsl.task") and
                 hasattr(e, "parsl.dependency_task")
                 ]

    # these pairs follow the dependency graph in the
    # invocation above.
    assert (b.tid, a.tid) in dep_pairs
    assert (c.tid, a.tid) in dep_pairs
    assert (d.tid, b.tid) in dep_pairs
    assert (d.tid, c.tid) in dep_pairs
    assert (e.tid, d.tid) in dep_pairs
    assert (e.tid, a.tid) not in dep_pairs
