import parsl
import pytest

from parsl.tests.configs.local_radical_mpi import fresh_config as local_config


@parsl.python_app
def test_mpi_func(comm, msg, sleep, ranks):
    import time
    msg = 'hello %d/%d: %s' % (comm.rank, comm.size, msg)
    time.sleep(sleep)
    return msg


apps = []


@pytest.mark.local
def test_radical_mpi(n=10):
    for i in range(n):
        t = test_mpi_func(None, msg='mpi.func.%06d' % i, sleep=1, ranks=2)
        apps.append(t)
    [app.result() for app in apps]
