import datetime
import json
import matplotlib.pyplot as plt
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.


if __name__ == "__main__":

    log = []

    with open("pytest-parsl/parsltest-current/runinfo/000/htex_Local/interchange.log", "r") as f:
        for l in f.readlines():
            log.append(json.loads(l))

    print(f"There are {len(log)} log entries") 

    print(f"Here's a random entry:")
    print(log[random.randint(0, len(log))])

    print("unix process analysis:")
    # assumption in this comprehension that every entry has a process ID
    ps = {l["process"] for l in log}
    print(ps)
    assert len(ps) == 1, "assume one process in parsl.log: the submit process"

    print("task analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    ts = {(l["htex_task_id"], ) for l in log if "htex_task_id" in l}
    print(f"there are {len(ts)} tasks")
    # for t in ts:
    #  print(f"Task: {t}")

    (task_id, ) = (list(ts))[random.randint(0, len(ts))]

    print(f"random chosen task: {task_id}")

    print(f"=== About task {task_id} ===")

    for l in log:
        if l.get('htex_task_id', None) == task_id:
            formatted_time = datetime.datetime.fromtimestamp(float(l['created']))
            print(formatted_time, l['formatted'])
