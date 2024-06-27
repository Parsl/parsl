import os
import signal

import pytest

import parsl
from parsl import Config, HighThroughputExecutor


@parsl.python_app
def get_manager_pgid():
    import os
    return os.getpgid(os.getpid())


@parsl.python_app
def lose_manager():
    import os
    import signal

    manager_pid = os.getppid()
    os.kill(manager_pid, signal.SIGSTOP)


@pytest.mark.local
def test_manager_lost_system_failure(tmpd_cwd):
    hte = HighThroughputExecutor(
        label="htex_local",
        address="127.0.0.1",
        max_workers_per_node=2,
        cores_per_worker=1,
        worker_logdir_root=str(tmpd_cwd),
        heartbeat_period=1,
        heartbeat_threshold=1,
    )
    c = Config(executors=[hte], strategy='simple', strategy_period=0.1)

    with parsl.load(c):
        manager_pgid = get_manager_pgid().result()
        try:
            lose_manager().result()
        except Exception as e:
            assert "ManagerLost" not in str(e), f"Issue 3495: {e}"
        finally:
            # Allow process to clean itself up
            os.killpg(manager_pgid, signal.SIGCONT)
            os.killpg(manager_pgid, signal.SIGTERM)
