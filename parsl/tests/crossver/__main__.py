import argparse
import contextlib
import importlib
import os
import pathlib
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, cast

from parsl.executors.high_throughput.protocol import OLDEST_SUPPORTED_PARSL

from .util import HERE, Combo, Env

if TYPE_CHECKING:
    from typing_extensions import Self

CROSSVER_DIR = pathlib.Path(__file__).resolve().parent
TESTS_DIR = CROSSVER_DIR.parents[0]
REPO_ROOT = CROSSVER_DIR.parents[2]


def latest_release() -> str:
    try:
        return subprocess.run(["git", "describe", "--tags", "--abbrev=0", "--match", "[0-9][0-9][0-9][0-9].[0-9][0-9].[0-9][0-9]"],
                              cwd=REPO_ROOT, check=True, capture_output=True, text=True).stdout.strip()
    except subprocess.CalledProcessError as e:
        sys.exit(f"crossver finds the latest release from this checkout's tags (in CI, check out with fetch-depth: 0): {e.stderr.strip()}")


PARSL_VERSIONS = (
    "2026.9.28",  # too old for any cross-version compatibility
    OLDEST_SUPPORTED_PARSL,
    latest_release(),
    HERE,
)
PYTHON_VERSIONS = ("3.10", "3.14")


@dataclass(frozen=True)
class Scenario:
    name: str
    compatible: Callable[[Combo, str], bool]


def discover_scenarios() -> List[Scenario]:
    scenarios = []
    for driver in sorted(CROSSVER_DIR.glob("*/driver.py")):
        name = driver.parent.name
        compatible = getattr(importlib.import_module(f".{name}", __package__), "compatible", None)
        if not callable(compatible):
            raise ValueError(f"{name}/__init__.py must define compatible(combo, oldest)")
        scenarios.append(Scenario(name, compatible))
    return scenarios


class VenvBuilder:
    def __init__(self, root: pathlib.Path) -> None:
        self.root = root
        self.built: Dict[Env, pathlib.Path] = {}
        self.here_sdist: Optional[pathlib.Path] = None

    def _here_requirement(self) -> str:
        if self.here_sdist is None:
            out_dir = self.root / "here-sdist"
            shutil.rmtree(out_dir, ignore_errors=True)
            subprocess.run(["uv", "build", "--sdist", "--out-dir", str(out_dir), str(REPO_ROOT)], check=True)
            [self.here_sdist] = out_dir.glob("*.tar.gz")
        return str(self.here_sdist)

    def venv(self, env: Env) -> pathlib.Path:
        if env in self.built:
            return self.built[env]
        venv = self.root / str(env)
        ready = venv / ".crossver-ready"
        if env.parsl == HERE or not ready.exists():
            print(f"=== building {env} at {venv}", flush=True)
            requirement = self._here_requirement() if env.parsl == HERE else f"parsl=={env.parsl}"
            shutil.rmtree(venv, ignore_errors=True)
            subprocess.run(["uv", "venv", "--python", f"{env.python}+gil", str(venv)], check=True)
            subprocess.run(["uv", "pip", "install", "--python", str(venv / "bin" / "python"), requirement], check=True)
            ready.touch()
        self.built[env] = venv
        return venv


class ManagedProcess(contextlib.AbstractContextManager):
    def __init__(self, argv: List[str], *, venv: pathlib.Path, cwd: pathlib.Path, logfile: pathlib.Path):
        self.argv = argv
        self.venv = venv
        self.cwd = cwd
        self.logfile = logfile

        self._proc: Optional[subprocess.Popen] = None

    def __str__(self) -> str:
        pid = f" [pid {self._proc.pid}]" if self._proc else ""
        return f"ManagedProcess({shlex.join(self.argv)}){pid}"

    def __enter__(self: "Self") -> "Self":
        environ = dict(os.environ)
        environ.pop("VIRTUAL_ENV", None)
        environ["PATH"] = f"{self.venv / 'bin'}{os.pathsep}{environ['PATH']}"

        self.cwd.mkdir(parents=True, exist_ok=True)
        self.logfile.parent.mkdir(parents=True, exist_ok=True)

        with open(self.logfile, "wb") as logfile:
            self._proc = subprocess.Popen(self.argv, cwd=self.cwd, env=environ, stdout=logfile, stderr=subprocess.STDOUT,
                                          start_new_session=True)
            return self

    def __exit__(self,
                 _exc_type: Optional[type[BaseException]],
                 _exc_value: Optional[BaseException],
                 _traceback: Optional[TracebackType]) -> None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.proc.pid, signal.SIGTERM)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    @property
    def proc(self) -> subprocess.Popen:
        if not self._proc:
            raise RuntimeError(f"{self} has no process; it either hasn't been started or is already finished.")
        return self._proc

    @property
    def exited(self) -> bool:
        return self.proc.poll() is not None

    @property
    def failed(self) -> bool:
        return self.exited and self.returncode != 0

    @property
    def returncode(self) -> Optional[int]:
        return self.proc.returncode


def wait_for(description: str, predicate: Callable[[], bool], *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {description}")
        time.sleep(0.1)


def run(scenario: Scenario, submit_venv: pathlib.Path, worker_venv: pathlib.Path, run_dir: pathlib.Path) -> None:
    launch_cmd = run_dir / "launch_cmd"

    driver_argv = [str(submit_venv / "bin" / "python"), "-m", f"{__package__}.{scenario.name}.driver", str(run_dir)]
    with ManagedProcess(driver_argv, venv=submit_venv, cwd=TESTS_DIR, logfile=run_dir / "driver.log") as driver:
        wait_for("the driver's launch command", lambda: launch_cmd.exists() or driver.exited, timeout=60)
        assert not driver.exited, f"driver exited with {driver.returncode} before writing a launch command"

        pool_argv = ["/bin/bash", "-c", launch_cmd.read_text()]
        with ManagedProcess(pool_argv, venv=worker_venv, cwd=run_dir / "worker", logfile=run_dir / "worker" / "pool.log") as pool:
            wait_for("the driver to exit", lambda: driver.exited or pool.failed, timeout=120)
            assert not pool.failed, f"pool exited with {pool.returncode}"

        assert driver.returncode == 0, f"driver exited with {driver.returncode}"


VERSION_SELECTIONS: Dict[str, Callable[[Combo], bool]] = {
    "all": lambda c: True,
    "involving-here": lambda c: any(env.parsl == HERE for env in (c.worker_env, c.submit_env)),
    "here-only": lambda c: all(env.parsl == HERE for env in (c.worker_env, c.submit_env)),
}


ALL_ENVS = [Env(p, py) for p in PARSL_VERSIONS for py in PYTHON_VERSIONS]
ALL_COMBOS = [Combo(submit_env=s, worker_env=w) for s in ALL_ENVS for w in ALL_ENVS]


def main(argv: Optional[List[str]] = None) -> int:
    def exit_on_signal(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, exit_on_signal)
    signal.signal(signal.SIGHUP, exit_on_signal)

    scenarios = discover_scenarios()

    parser = argparse.ArgumentParser(description="Run HTEX components from different Parsl/Python environments against each other.")
    parser.add_argument("--work-dir", type=pathlib.Path, default=REPO_ROOT / ".crossver",
                        help="where environments (reused between runs) and per-run logs go")
    parser.add_argument("--versions", choices=list(VERSION_SELECTIONS), default="all",
                        help="which Parsl version combinations to run (default: all); HERE is this checkout")
    parser.add_argument("--scenario", action="append", choices=[s.name for s in scenarios], help="may be repeated; default: all")
    parser.add_argument("--dry-run", action="store_true", help="list combinations without running them")
    args = parser.parse_args(argv)

    selected = [(scenario, combo)
                for scenario in scenarios
                if not args.scenario or scenario.name in args.scenario
                for combo in ALL_COMBOS
                if VERSION_SELECTIONS[args.versions](combo) and scenario.compatible(combo, OLDEST_SUPPORTED_PARSL)]

    if not selected:
        print("no combinations selected", file=sys.stderr)
        return 2
    if args.dry_run:
        for scenario, combo in selected:
            print(f"{scenario.name}[{combo}]")
        return 0

    work_dir = cast(pathlib.Path, args.work_dir).resolve()
    venv_builder = VenvBuilder(work_dir / "envs")
    runs_root = work_dir / "runs" / time.strftime("%Y%m%d-%H%M%S")
    failures: List[Tuple[str, pathlib.Path]] = []

    for scenario, combo in selected:
        label = f"{scenario.name}[{combo}]"
        print(f"=== {label}", flush=True)
        run_dir = runs_root / scenario.name / str(combo)
        start = time.monotonic()
        try:
            run(scenario, venv_builder.venv(combo.submit_env), venv_builder.venv(combo.worker_env), run_dir)
        except Exception:
            traceback.print_exc()
            lines = 20
            for log in sorted(run_dir.rglob("*.log")):
                print(f"--- {log.resolve()} (last {lines} lines)")
                print("\n".join(log.read_text(errors="replace").splitlines()[-lines:]))
            print(f"FAIL {label} ({time.monotonic() - start:.1f}s); logs in {run_dir}", flush=True)
            failures.append((label, run_dir))
        else:
            print(f"PASS {label} ({time.monotonic() - start:.1f}s)", flush=True)

    print(f"=== {len(selected) - len(failures)}/{len(selected)} passed")
    for label, run_dir in failures:
        print(f"FAIL {label}: {run_dir}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
