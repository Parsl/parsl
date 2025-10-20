import datetime
import json
import matplotlib.pyplot as plt
import os
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.

if __name__ == "__main__":
    from parsl.observability.getlogs import getlogs

    log = getlogs()

    print(f"There are {len(log)} log entries") 

    print(f"Here's a random entry:")
    print(log[random.randint(0, len(log))])

    print("unix process analysis:")
    # assumption in this comprehension that every entry has a process ID
    ps = {l["process"] for l in log if "process" in l}
    print(ps)
    # assert len(ps) == 1, "assume one process in parsl.log: the submit process"

    print("task analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    ts = {(l["parsl_task_id"], l["parsl_dfk"]) for l in log if "parsl_task_id" in l}
    print(f"there are {len(ts)} tasks")
    # for t in ts:
    #  print(f"Task: {t}")

    (task_id, dfk) = (list(ts))[random.randint(0, len(ts))]

    print(f"random chosen task: {task_id}")

    print(f"=== About task {task_id}/{type(task_id)} from DFK {dfk} ===")

    tasklog = [l for l in log if l.get('parsl_dfk', None) == dfk and str(l.get('parsl_task_id', None)) == str(task_id)]
    tasklog.sort(key=lambda l: float(l['created']))
    for l in tasklog:
        formatted_time = datetime.datetime.fromtimestamp(float(l['created']))
        actor = ""
        if 'process' in l:
            actor += l['process'] 
        if 'threadName' in l:
            action = l['threadName'] + "@" + actor
        print(formatted_time, actor, l['formatted'], f"({l['logsource']})")
