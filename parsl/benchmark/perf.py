import importlib
import time
import concurrent.futures
import parsl


# TODO: parameterise
res=None
# res = {"cores":1, "memory":0, "disk":0}


# TODO: factor with conftest.py where this is copy/pasted from?
def load_dfk_from_config(filename):
    spec = importlib.util.spec_from_file_location('', filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if hasattr(module, 'config'):
        dfk = parsl.load(module.config)
    elif hasattr(module, 'fresh_config'):
        dfk = parsl.load(module.fresh_config())
    else:
        raise RuntimeError("Config module does not define config or fresh_config")


@parsl.python_app
def app(parsl_resource_specification={}):
    return 7

load_dfk_from_config("parsl/tests/configs/local_threads.py")

n = 10

delta_t = 0

target_t = 120  # 2 minutes
threshold_t = int(0.75 * target_t)

while delta_t < threshold_t:
    print("=======================================================")
    print(f"Will run {n} tasks to target {target_t} seconds runtime")
    start_t = time.time()

    fs = []
    print("Submitting tasks / invoking apps")
    for _ in range(n):
        fs.append(app(parsl_resource_specification=res))

    submitted_t = time.time()
    print(f"All {n} tasks submitted ... waiting for completion")
    print(f"Submission took {submitted_t - start_t} seconds = {n/(submitted_t - start_t)} tasks/second")

    for f in concurrent.futures.as_completed(fs):
        assert f.result() == 7

    end_t = time.time()

    delta_t = end_t - start_t

    rate = n/delta_t

    print(f"Runtime: {delta_t}s vs target {target_t}")
    print(f"Tasks per second: {rate}")

    n = int(target_t * rate)

print("Cleaning up DFK")
parsl.dfk().cleanup()
print("The end")
