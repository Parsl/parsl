import argparse
import concurrent.futures
import importlib
import json
import platform
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.errors import InternalConsistencyError

VALID_NAMED_ITERATION_MODES = ("estimate", "exponential")

min_iterations = 2


# TODO: factor with conftest.py where this is copy/pasted from?
def load_dfk_from_config(filename: str) -> DataFlowKernel:
    spec = importlib.util.spec_from_file_location('', filename)

    if spec is None:
        raise RuntimeError("Could not import configuration")

    module = importlib.util.module_from_spec(spec)

    if spec.loader is None:
        raise RuntimeError("Could not load configuration")

    spec.loader.exec_module(module)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'config'):
        return parsl.load(module.config)
    elif hasattr(module, 'fresh_config'):
        return parsl.load(module.fresh_config())
    else:
        raise RuntimeError("Config module does not define config or fresh_config")


@parsl.python_app
def app(extra_payload: Any, parsl_resource_specification: Dict = {}) -> int:
    return 7


def performance(*, resources: dict, target_t: float, args_extra_size: int, iterate_mode: str | list[int],
                record_iteration: Callable[[Dict[str, Any]], None] | None = None) -> None:

    delta_t: float

    iteration = 1

    args_extra_payload = "x" * args_extra_size

    if isinstance(iterate_mode, list):
        n = iterate_mode[0]
    else:
        n = 10

    iterate = True

    while iterate:
        print(f"==== Iteration {iteration} ====")
        print(f"Will run {n} tasks")
        start_t = time.time()

        fs = []
        print("Submitting tasks / invoking apps")
        for _ in range(n):
            fs.append(app(args_extra_payload, parsl_resource_specification=resources))

        submitted_t = time.time()
        print(f"All {n} tasks submitted ... waiting for completion")
        print(f"Submission took {submitted_t - start_t:.3f} seconds = {n / (submitted_t - start_t):.3f} tasks/second")

        for f in concurrent.futures.as_completed(fs):
            assert f.result() == 7

        end_t = time.time()

        delta_t = end_t - start_t

        rate = n / delta_t

        print(f"Runtime: actual {delta_t:.3f}s vs target {target_t}s")
        print(f"Tasks per second: {rate:.3f}")

        if record_iteration is not None:
            record_iteration({
                "iteration": iteration,
                "task_count": n,
                "start_timestamp": start_t,
                "submitted_timestamp": submitted_t,
                "end_timestamp": end_t,
                "submission_seconds": submitted_t - start_t,
                "submission_tasks_per_second": n / (submitted_t - start_t),
                "runtime_seconds": delta_t,
                "tasks_per_second": rate,
            })

        iteration += 1

        # decide upon next iteration

        match iterate_mode:
            case "estimate":
                n = max(1, int(target_t * rate))
                iterate = delta_t < (0.75 * target_t) or iteration <= min_iterations
            case "exponential":
                n = int(n * 2)
                iterate = delta_t < target_t or iteration <= min_iterations
            case seq if isinstance(seq, list) and iteration <= len(seq):
                n = seq[iteration - 1]
                iterate = True
            case seq if isinstance(seq, list):
                iterate = False
            case _:
                raise InternalConsistencyError(f"Bad iterate mode {iterate_mode} - should have been validated at arg parse time")


def validate_int_list(v: str) -> list[int] | Literal[False]:
    try:
        return list(map(int, v.split(",")))
    except ValueError:
        return False


def iteration_mode(v: str) -> str | list[int]:
    match v:
        case s if s in VALID_NAMED_ITERATION_MODES:
            return s
        case _ if seq := validate_int_list(v):
            return seq
        case _:
            raise argparse.ArgumentTypeError(f"Invalid iteration mode: {v}")


def cli_run() -> None:
    parser = argparse.ArgumentParser(
        prog="parsl-perf",
        description="Measure performance of Parsl configurations",
        epilog="""
Example usage: python -m parsl.benchmark.perf --config parsl/tests/configs/workqueue_blocks.py  --resources '{"cores":1, "memory":0, "disk":0}'
        """)

    parser.add_argument("--config", required=True, help="path to Python file that defines a configuration")
    parser.add_argument("--resources", metavar="EXPR", help="parsl_resource_specification dictionary")
    parser.add_argument("--time", metavar="SECONDS", help="target number of seconds for an iteration", default=120, type=float)
    parser.add_argument("--argsize", metavar="BYTES", help="extra bytes to add into app invocation arguments", default=0, type=int)
    parser.add_argument("--version", action="version", version=f"parsl-perf from Parsl {parsl.__version__}")
    parser.add_argument("--json-output", action="store_true",
                        help="also save results and run metadata to a timestamped JSON file")
    parser.add_argument("--output-dir", type=Path, default=Path("parsl-perf-output"), metavar="PATH",
                        help="JSON output directory, relative to the current directory (default: parsl-perf-output; requires --json-output)")
    parser.add_argument("--output-file", metavar="NAME",
                        help="JSON filename within --output-dir (default: UTC timestamped name; requires --json-output)")
    parser.add_argument("--iterate",
                        metavar="MODE",
                        help="Iteration mode: " + ", ".join(VALID_NAMED_ITERATION_MODES) + ", or sequence of explicit sizes",
                        type=iteration_mode,
                        default="estimate")

    args = parser.parse_args()

    if args.output_file is not None:
        if not args.json_output:
            parser.error("--output-file requires --json-output")
        if not args.output_file or Path(args.output_file).name != args.output_file:
            parser.error("--output-file must be a filename only; use --output-dir for directories")

    if args.resources:
        resources = eval(args.resources)
    else:
        resources = {}

    if args.json_output:
        run_with_json_report(args, resources, parser)
    else:
        run_benchmark(args, resources)
    print("The end")


def run_benchmark(args: argparse.Namespace, resources: dict,
                  record_iteration: Callable[[Dict[str, Any]], None] | None = None) -> None:
    with load_dfk_from_config(args.config):
        performance(resources=resources, target_t=args.time, args_extra_size=args.argsize,
                    iterate_mode=args.iterate, record_iteration=record_iteration)
        print("Tests complete - leaving DFK block")


def publish_report(path: Path, contents: str, *, create: bool = False) -> None:
    # Stage beside the destination so readers always see a complete JSON document.
    staged_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".parsl-perf-", suffix=".tmp", delete=False) as staged:
            staged_path = Path(staged.name)
            staged.write(contents)
            staged.write("\n")
        if create:
            # Publish the first report only if this run owns the destination name.
            path.hardlink_to(staged_path)
        else:
            staged_path.replace(path)
    finally:
        if staged_path is not None:
            staged_path.unlink(missing_ok=True)


def run_with_json_report(args: argparse.Namespace, resources: dict, parser: argparse.ArgumentParser) -> None:
    results: list[Dict[str, Any]] = []
    started_at = datetime.now().astimezone()
    report = {
        "schema_version": 1,
        "started_at": started_at.isoformat(),
        "parsl_version": parsl.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "configuration": {
            "config_path": str(Path(args.config).resolve()),
            "iteration_mode": args.iterate,
            "target_time_seconds": args.time,
            "argument_size_bytes": args.argsize,
            "resources": resources,
        },
        "status": "in-progress",
        "iterations": results,
    }
    # Check metadata before running the benchmark, which may take a long time.
    try:
        initial_contents = json.dumps(report, indent=2, allow_nan=False)
    except (TypeError, ValueError) as exc:
        parser.error(f"cannot serialize JSON run metadata: {exc}")

    timestamp = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    filename = args.output_file if args.output_file is not None else f"parsl_perf_{timestamp}.json"
    output_path = args.output_dir / filename
    try:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        publish_report(output_path, initial_contents, create=True)
    except OSError as exc:
        parser.error(f"cannot create JSON output file: {exc}")

    def save_report() -> None:
        publish_report(output_path, json.dumps(report, indent=2, allow_nan=False))

    def record_iteration(iteration: Dict[str, Any]) -> None:
        results.append(iteration)
        save_report()

    try:
        run_benchmark(args, resources, record_iteration)
    except BaseException as exc:
        report["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        report["finished_at"] = datetime.now().astimezone().isoformat()
        try:
            save_report()
        except (OSError, TypeError, ValueError) as write_error:
            print(f"Could not save JSON results: {write_error}", file=sys.stderr)
        else:
            print(f"JSON results saved to: {output_path.resolve()}")
        raise

    report["status"] = "completed"
    report["finished_at"] = datetime.now().astimezone().isoformat()
    save_report()
    print(f"JSON results saved to: {output_path.resolve()}")


if __name__ == "__main__":
    cli_run()
