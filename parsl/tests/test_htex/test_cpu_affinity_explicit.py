import logging
import os
import random

import pytest

import parsl
from parsl.tests.configs.htex_local import fresh_config

logger = logging.getLogger(__name__)


@parsl.python_app
def my_affinity():
    """an app that returns the affinity of the unix process it is currently in.
    """
    import os
    return os.sched_getaffinity(0)


@pytest.mark.local
@pytest.mark.multiple_cores_required
@pytest.mark.skipif('sched_getaffinity' not in dir(os), reason='System does not support sched_setaffinity')
def test_cpu_affinity_explicit():
    available_cores = os.sched_getaffinity(0)

    logger.debug(f"Got these cores: {available_cores}")

    assert len(available_cores) >= 2, "This test requires multiple cores. Run with '-k not multiple_cores' to skip"

    cores_as_list = list(available_cores)

    single_core = random.choice(cores_as_list)
    affinity = f"list:{single_core}"

    logger.debug(f"Will test with affinity for one worker, one core: {affinity}")

    config = fresh_config()
    config.executors[0].cpu_affinity = affinity
    config.executors[0].max_workers_per_node = 1

    logger.debug(f"config: {config}")

    with parsl.load(config):
        worker_affinity = my_affinity().result()
        logger.debug(f"worker reported this affinity: {worker_affinity}")
        assert len(worker_affinity) == 1
        assert worker_affinity == set((single_core,))
