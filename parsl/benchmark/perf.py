import argparse
import concurrent.futures
import importlib
import time

from typing import Optional

import parsl

min_iterations = 2


# TODO: factor with conftest.py where this is copy/pasted from?
def load_dfk_from_config(filename):
    spec = importlib.util.spec_from_file_location('', filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'config'):
        return parsl.load(module.config)
    elif hasattr(module, 'fresh_config'):
        return parsl.load(module.fresh_config())
    else:
        raise RuntimeError("Config module does not define config or fresh_config")


@parsl.python_app
def app(extra_payload, parsl_resource_specification={}):
    return 7


def performance(*, resources: dict, target_t: float, args_extra_size: int, count: Optional[int]):
    n = 10

    delta_t: float
    delta_t = 0

    threshold_t = target_t

    iteration = 1

    args_extra_payload = "x" * args_extra_size

    while (delta_t < threshold_t and count is None) or iteration <= min_iterations:
        print(f"==== Iteration {iteration} ====")

        # skip forcing count on first iteration
        if count is not None and iteration != 1:
            assert isinstance(count, int)
            n = count

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

        # n = max(1, int(target_t * rate))
        # I'm suspicious about performance dropping off as task numbers get bigger:
        # i've seen that with WQ, for example,
        # so this hack, which could be an optional mode, performs a determininistic
        # increase in numbers of tasks up to the time limit - so that non-linear behaviour
        # can be seen a bit better - this is specially for comparing parsl-perf runs which
        # otherwise might be picking a random sweet-ish spot...
        # the threshold, in this case, could also be expressed in terms of task count,
        # rather than max time (to give the same number of readings, rather than up to around
        # the same task time) but using time can give more constant execution time in the
        # presence of wildly different per-task performances (which is the case with parsl...)
        # switching to these modes could all be parameters...

        n = int(n * 1.4142135623730951)  # (doesn't have to be 2... could also be parameterised... sqrt(2) would be nice?)

        iteration += 1


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
    parser.add_argument("--count",
                        metavar="TASKS",
                        help="target number of tasks for an iteration (beyond warmups)", default=None, type=int)
    # TODO: should be mutually exclusive with --time

    parser.add_argument("--argsize", metavar="BYTES", help="extra bytes to add into app invocation arguments", default=0, type=int)
    parser.add_argument("--version", action="version", version=f"parsl-perf from Parsl {parsl.__version__}")

    args = parser.parse_args()

    if args.resources:
        resources = eval(args.resources)
    else:
        resources = {}

    with load_dfk_from_config(args.config):
        performance(resources=resources, target_t=args.time, args_extra_size=args.argsize, count=args.count)
        print("Tests complete - leaving DFK block")
    print("The end")


if __name__ == "__main__":
    cli_run()
