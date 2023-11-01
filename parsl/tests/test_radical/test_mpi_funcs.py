import parsl
import pytest

from parsl.tests.configs.local_radical_mpi import fresh_config as local_config


@parsl.python_app
def test_mpi_func(comm, msg, sleep, ranks):
    import time
    msg = 'hello %d/%d: %s' % (comm.rank, comm.size, msg)
    time.sleep(sleep)
    print(msg)
    return comm.size


apps = []


@pytest.mark.local
def test_radical_mpi(n=7):
    # rank size should be > 1 for the
    # radical runtime system to run this function in MPI env
    for i in range(2, n):
        t = test_mpi_func(None, msg='mpi.func.%06d' % i, sleep=1, ranks=i)
        apps.append(t)
    assert [len(app.result()) for app in apps] == list(range(2, n))
