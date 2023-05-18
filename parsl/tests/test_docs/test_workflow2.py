import time

import pytest

from parsl.app.app import python_app
from parsl.tests.configs.local_threads import config


local_config = config


@python_app
def wait_sleep_double(x, fu_1, fu_2):
    import time
    time.sleep(2)   # Sleep for 2 seconds
    return x * 2


@pytest.mark.skip('fails with pytest+xdist')
def test_parallel(N=2):
    """Parallel workflow example from docs on composing a workflow
    """

    # Launch two apps, which will execute in parallel, since they don't have to
    # wait on any futures
    start = time.time()
    doubled_x = wait_sleep_double(N, None, None)
    doubled_y = wait_sleep_double(N, None, None)

    # The third depends on the first two :
    #    doubled_x   doubled_y     (2 s)
    #           \     /
    #           doublex_z          (2 s)
    doubled_z = wait_sleep_double(N, doubled_x, doubled_y)

    # doubled_z will be done in ~4s
    print(doubled_z.result())
    time.time()
    delta = time.time() - start

    assert doubled_z.result() == N * \
        2, "Expected doubled_z = N*2 = {0}".format(N * 2)
    assert delta > 4 and delta < 5, "Time delta exceeded expected 4 < duration < 5"
