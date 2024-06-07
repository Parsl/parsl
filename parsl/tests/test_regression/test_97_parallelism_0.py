import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider


def local_config() -> Config:
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                worker_debug=True,
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=1,
                    parallelism=0,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy='simple',
    )


@parsl.python_app
def python_app():
    return 7


@pytest.mark.local
def test_parallism_0():
    """This is a regression test for parsl issue #97,
    https://github.com/Parsl/parsl/issues/97

    In that issue, if parallelism is set to 0, then no blocks would be
    provisioned by the simple scaling strategy, as the number of blocks
    to be scaled out would include a parallelism factor of 0 and so would
    always be 0. With 0 blocks, no tasks would execute.

    Commit 2828a779e00bf0188ec82454c9aa3004480b5809 adds in an additional
    scaling strategy case to make one block be scaled out when there are
    active tasks, even if the parallelism-based path says 0 blocks are
    needed.

    This test runs a single app to completion. This test would hang in the
    case of a regression.
    """
    assert python_app().result() == 7
