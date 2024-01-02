import contextlib
import os
import pytest
import parsl
from parsl import python_app

from parsl.tests.configs.local_threads_checkpoint import fresh_config


@contextlib.contextmanager
def parsl_configured(run_dir, **kw):
    c = fresh_config()
    c.run_dir = run_dir
    for config_attr, config_val in kw.items():
        setattr(c, config_attr, config_val)
    dfk = parsl.load(c)
    for ex in dfk.executors.values():
        ex.working_dir = run_dir
    yield dfk

    parsl.dfk().cleanup()
    parsl.clear()


@python_app(cache=True)
def random_app(i):
    import random
    return random.randint(i, 100000)


def launch_n_random(n=2):
    """1. Launch a few apps and write the checkpoint once a few have completed
    """
    d = [random_app(i) for i in range(0, n)]
    return [i.result() for i in d]


@pytest.mark.local
def test_loading_checkpoint(tmpd_cwd, n=4):
    """Load memoization table from previous checkpoint
    """
    with parsl_configured(tmpd_cwd, checkpoint_mode="task_exit"):
        checkpoint_files = [os.path.join(parsl.dfk().run_dir, "checkpoint")]
        results = launch_n_random(n)

    with parsl_configured(tmpd_cwd, checkpoint_files=checkpoint_files):
        relaunched = launch_n_random(n)

    assert len(relaunched) == len(results) == n, "Expected all results to have n items"
    for i in range(n):
        assert relaunched[i] == results[i], "Expect relaunch to find cached results"
