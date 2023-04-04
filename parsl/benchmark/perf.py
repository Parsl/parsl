import time
import concurrent.futures
import parsl

# using desc branch 8db4605ab330670e2e19670697efaceeeaf7075e
# so likely substantial performance changes (especially around
# monitoring) compared to master.

# on login25.perlmutter, Tasks per second: 2727.5157973484197
# Thu 23 Mar 2023 11:52:25 AM PDT
from parsl.tests.configs.local_threads import fresh_config
res=None

# ndcctools-7.5.0 
# login25.perlmutter
# just running a single worker on the login node
# Tasks per second: 2.369018093557212
# from parsl.tests.configs.workqueue_blocks import config

# as above, but on an salloced interactive cpu node
# Tasks per second: 2.4820457425859237
# salloc --nodes 1 --qos interactive --time 01:00:00 --constraint cpu

# now, set to use one core per task instead of whole node
# again on salloc node, again with wq
# res = {"cores":1, "memory":0, "disk":0}
# in three runs in a row, quite serious variation:
# Tasks per second: 16.912363985744122
# Tasks per second: 31.96491452408092
# Tasks per second: 30.249980146631163

# This ^ configuration is probably interesting to examine using
# dnpcsql...
# it's a 15x speedup on 256x cores...
# so *something* must be taking substantially more time, and I
# wonder what?

# from parsl.tests.configs.workqueue_blocks import config
# res = {"cores":1, "memory":0, "disk":0}

# def fresh_config():
#    return config

@parsl.python_app
def app(parsl_resource_specification={}):
    return 7

parsl.load(fresh_config())

n = 10

delta_t = 0

target_t = 120  # 2 minutes

while delta_t < 90:
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

print("The end")
