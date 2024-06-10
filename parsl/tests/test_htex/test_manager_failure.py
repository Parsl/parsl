import os
import signal
import time

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


@pytest.fixture(autouse=True, scope="function")
def load_config():
    config = fresh_config()
    config.executors[0].poll_period = 1
    config.executors[0].max_workers_per_node = 1
    config.executors[0].heartbeat_period = 1

    parsl.load(config)
    yield

    parsl.dfk().cleanup()


@python_app
def get_worker_pid():
    import os
    return os.getpid()


@python_app
def kill_manager(sig: int):
    import os
    os.kill(os.getppid(), sig)


@pytest.mark.local
@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGKILL])
def test_htex_manager_failure_worker_shutdown(sig: int):
    """Ensure that HTEX workers shut down when the Manager process dies."""
    worker_pid = get_worker_pid().result()

    kill_manager(sig)

    with pytest.raises(OSError):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            # Raises an exception if the process
            # does not exist
            os.kill(worker_pid, 0)
            time.sleep(.1)
