import argparse
import concurrent.futures
import importlib
import time
from typing import Any, Dict

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.errors import InternalConsistencyError

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


def performance(*, resources: dict, target_t: float, args_extra_size: int, iterate_mode: str) -> None:
    n = 10

    delta_t: float

    iteration = 1

    args_extra_payload = "x" * args_extra_size

    iterate = True

    while iterate:
        print(f"==== Iteration {iteration} ====")
        print(f"Will run {n} tasks to target {target_t} seconds runtime")
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

        iteration += 1

        # decide upon next iteration

        match iterate_mode:
            case "estimate":
                n = max(1, int(target_t * rate))
                iterate = delta_t < (0.75 * target_t) or iteration <= min_iterations
            case "exponential":
                n = int(n * 2)
                iterate = delta_t < target_t or iteration <= min_iterations
            case _:
                raise InternalConsistencyError(f"Bad iterate mode {iterate_mode} - should have been validated at arg parse time")


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
    parser.add_argument("--iterate",
                        metavar="MODE",
                        help="Iteration mode: estimate, exponential",
                        type=str,
                        default="estimate",
                        choices=("estimate", "exponential"))

    args = parser.parse_args()

    if args.resources:
        resources = eval(args.resources)
    else:
        resources = {}

    with load_dfk_from_config(args.config):
        performance(resources=resources, target_t=args.time, args_extra_size=args.argsize, iterate_mode=args.iterate)
        print("Tests complete - leaving DFK block")
    print("The end")


if __name__ == "__main__":
    cli_run()
