import argparse
import concurrent.futures
import importlib
import json
import platform
import sys
import time
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
                results: list[Dict[str, Any]] | None = None) -> None:

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

        if results is not None:
            results.append({
                "iteration": iteration,
                "task_count": n,
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
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"), metavar="PATH",
                        help="JSON output directory, relative to the current directory (default: outputs; requires --json-output)")
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
        if not args.output_file or args.output_file in (".", "..") or Path(args.output_file).name != args.output_file:
            parser.error("--output-file must be a filename only; use --output-dir for directories")

    if args.resources:
        resources = eval(args.resources)
    else:
        resources = {}

    output_file = None
    results: list[Dict[str, Any]] = []
    started_at = datetime.now().astimezone()
    report: Dict[str, Any] = {}
    if args.json_output:
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
            "status": "completed",
            "iterations": results,
        }
        try:
            json.dumps(report, allow_nan=False)
        except (TypeError, ValueError) as exc:
            parser.error(f"cannot serialize JSON run metadata: {exc}")
        try:
            args.output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            filename = args.output_file if args.output_file is not None else f"parsl_perf_{timestamp}.json"
            output_path = args.output_dir / filename
            output_file = output_path.open("x", encoding="utf-8")
        except OSError as exc:
            parser.error(f"cannot create JSON output file: {exc}")

    try:
        with load_dfk_from_config(args.config):
            performance(resources=resources, target_t=args.time, args_extra_size=args.argsize,
                        iterate_mode=args.iterate, results=results if args.json_output else None)
            print("Tests complete - leaving DFK block")
    except BaseException as exc:
        # Preserve completed iterations, including on Ctrl+C or configuration/cleanup failure.
        report["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        if output_file is not None:
            report["finished_at"] = datetime.now().astimezone().isoformat()
            try:
                with output_file:
                    json.dump(report, output_file, indent=2, allow_nan=False)
                    output_file.write("\n")
                print(f"JSON results saved to: {output_path.resolve()}")
            except (OSError, TypeError, ValueError) as exc:
                if report["status"] == "completed":
                    raise
                # A reporting failure must not hide the original benchmark error.
                print(f"Could not save JSON results: {exc}", file=sys.stderr)
    print("The end")


if __name__ == "__main__":
    cli_run()
