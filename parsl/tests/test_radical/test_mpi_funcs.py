import pytest

import parsl
from parsl.tests.configs.local_radical_mpi import fresh_config as local_config


@parsl.python_app
def some_mpi_func(msg, sleep, comm=None, parsl_resource_specification={}):
    import time
    msg = 'hello %d/%d: %s' % (comm.rank, comm.size, msg)
    time.sleep(sleep)
    print(msg)
    return comm.size


apps = []


@pytest.mark.local
@pytest.mark.radical
def test_radical_mpi(n=7):
    # rank size should be > 1 for the
    # radical runtime system to run this function in MPI env
    for i in range(2, n):
        spec = {'ranks': i}
        t = some_mpi_func(msg='mpi.func.%06d' % i, sleep=1, comm=None, parsl_resource_specification=spec)
        apps.append(t)
    assert [len(app.result()) for app in apps] == list(range(2, n))
