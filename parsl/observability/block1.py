import datetime
import json
import matplotlib.pyplot as plt
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.

from parsl.observability.getlogs import getlogs

if __name__ == "__main__":

    log = getlogs()

    print(f"There are {len(log)} log entries") 

    print(f"Here's a random entry:")
    print(log[random.randint(0, len(log))])

    print("unix process analysis:")
    # assumption in this comprehension that every entry has a process ID
    ps = {l["process"] for l in log}
    print(ps)
    # assert len(ps) == 1, "assume one process in parsl.log: the submit process"

    print("task analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    ts = {(l["block_id"], l["parsl_executor"], l["parsl_dfk"]) for l in log if "block_id" in l and "parsl_dfk" in l and "parsl_executor" in l}
    print(f"there are {len(ts)} blocks")
    # for t in ts:
    #  print(f"Task: {t}")

    (block_id, executor_id, dfk) = random.choice(list(ts))

    print(f"random chosen block: {block_id}")

    print(f"=== About block {block_id} ===")

    tasklog = [l for l in log if l.get('parsl_dfk', None) == dfk and l.get('block_id', None) == block_id and l.get('parsl_executor', None) == executor_id]
    tasklog.sort(key=lambda l: l['created'])
    for l in tasklog:
            formatted_time = datetime.datetime.fromtimestamp(float(l['created']))
            print(formatted_time, l['msg'])
