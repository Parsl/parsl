import pytest

from parsl.app.app import python_app
from parsl.executors.high_throughput.errors import WorkerLost


def local_config():
    from parsl.tests.configs.htex_local import fresh_config
    config = fresh_config()
    config.executors[0].poll_period = 1
    config.executors[0].max_workers_per_node = 1
    config.executors[0].heartbeat_period = 1
    return config


@python_app
def kill_worker():
    raise SystemExit(2)


@pytest.mark.local
def test_htex_worker_failures():
    for _ in range(3):
        with pytest.raises(WorkerLost):
            f = kill_worker()
            f.result()
