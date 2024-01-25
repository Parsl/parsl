import logging
import os
import parsl
import pytest
import random
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
    config.executors[0].max_workers = 1

    logger.debug(f"config: {config}")
    # TODO: is there a `with` style for this, to properly deal with exceptions?

    parsl.load(config)
    try:

        worker_affinity = my_affinity().result()
        logger.debug(f"worker reported this affinity: {worker_affinity}")
        assert len(worker_affinity) == 1
        assert worker_affinity == set((single_core,))

    finally:
        parsl.dfk().cleanup()
        parsl.clear()
