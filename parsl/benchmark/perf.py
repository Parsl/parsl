import argparse
import importlib
import time
import concurrent.futures
import parsl


# TODO: factor with conftest.py where this is copy/pasted from?
def load_dfk_from_config(filename):
    spec = importlib.util.spec_from_file_location('', filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'config'):
        parsl.load(module.config)
    elif hasattr(module, 'fresh_config'):
        parsl.load(module.fresh_config())
    else:
        raise RuntimeError("Config module does not define config or fresh_config")


@parsl.python_app
def app(parsl_resource_specification={}):
    return 7


def performance(*, resources: dict):
    n = 10

    delta_t = 0

    target_t = 120  # 2 minutes
    threshold_t = int(0.75 * target_t)

    iteration = 1

    while delta_t < threshold_t:
        print(f"==== Iteration {iteration} ====")
        print(f"Will run {n} tasks to target {target_t} seconds runtime")
        start_t = time.time()

        fs = []
        print("Submitting tasks / invoking apps")
        for _ in range(n):
            fs.append(app(parsl_resource_specification=resources))

        submitted_t = time.time()
        print(f"All {n} tasks submitted ... waiting for completion")
        print(f"Submission took {submitted_t - start_t} seconds = {n/(submitted_t - start_t)} tasks/second")

        for f in concurrent.futures.as_completed(fs):
            assert f.result() == 7

        end_t = time.time()

        delta_t = end_t - start_t

        rate = n / delta_t

        print(f"Runtime: {delta_t}s vs target {target_t}")
        print(f"Tasks per second: {rate}")

        n = int(target_t * rate)

        iteration += 1

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="parsl-perf",
        description="Measure performance of Parsl configurations",
        epilog="Example usage: python -m parsl.benchmark.perf --config parsl/tests/configs/workqueue_blocks.py  --resources '{\"cores\":1, \"memory\":0, \"disk\":0}'")

    parser.add_argument("--config", required=True, help="path to Python file that defines a configuration")
    parser.add_argument("--resources", metavar="EXPR", help="parsl_resource_specification dictionary")

    args = parser.parse_args()

    if args.resources:
        resources = eval(args.resources)
    else:
        resources = {}

    load_dfk_from_config(args.config)
    performance(resources = resources)
    print("Cleaning up DFK")
    parsl.dfk().cleanup()
    print("The end")
