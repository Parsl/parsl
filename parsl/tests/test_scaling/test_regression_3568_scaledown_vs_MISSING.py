import parsl
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import WrappedLauncher
from parsl.providers import LocalProvider

import pytest

def local_config():
    return Config(
        max_idletime = 6,
        strategy = 'htex_auto_scale',
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                worker_debug=True,
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=20,
                    min_blocks=0,
                    max_blocks=20,
                    launcher=WrappedLauncher(prepend="sleep 60 ; "),
                ),
            )
        ],
    )


@parsl.python_app
def task():
  return 7

@pytest.mark.local
def test_regression():
  print("waiting")
  import time
  time.sleep(120)
  print("task2")
  assert task().result() == 7
