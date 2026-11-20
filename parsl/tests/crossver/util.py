import contextlib
import os
import pathlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple

from typing_extensions import Generator

if TYPE_CHECKING:
    from parsl.dataflow.dflow import DataFlowKernel

HERE = "HERE"


@dataclass(frozen=True)
class Env:
    parsl: str
    python: str

    def __str__(self) -> str:
        return f"parsl-{self.parsl}-py{self.python}"


@dataclass(frozen=True)
class Combo:
    submit_env: Env
    worker_env: Env

    def __str__(self) -> str:
        return f"submit-{self.submit_env},worker-{self.worker_env}"


def supported(env: Env, oldest: str) -> bool:
    def release(v: str) -> Tuple[int, ...]:
        return tuple(int(part) for part in v.split("."))

    return env.parsl == HERE or release(env.parsl) >= release(oldest)


def interoperable(combo: Combo, oldest: str) -> bool:
    (submit, worker) = combo.submit_env, combo.worker_env
    return submit.parsl == worker.parsl or (supported(submit, oldest) and supported(worker, oldest))


@contextlib.contextmanager
def load(run_dir: pathlib.Path, **htex_kwargs) -> Generator["DataFlowKernel"]:
    import parsl
    from parsl.config import Config
    from parsl.executors import HighThroughputExecutor
    from parsl.providers import LocalProvider

    htex = HighThroughputExecutor(address="127.0.0.1",
                                  encrypted=False,
                                  max_workers_per_node=2,
                                  heartbeat_period=2,
                                  provider=LocalProvider(init_blocks=0,
                                                         min_blocks=0,
                                                         max_blocks=0),
                                  **htex_kwargs)
    if hasattr(htex, "_check_python_mismatch"):
        htex._check_python_mismatch = False

    with parsl.load(Config(executors=[htex], run_dir=str(run_dir / "runinfo"), strategy="none")) as dfk:
        tmp = run_dir / "launch_cmd.tmp"
        tmp.write_text(htex.launch_cmd.format(block_id=0))
        os.rename(tmp, run_dir / "launch_cmd")
        yield dfk
