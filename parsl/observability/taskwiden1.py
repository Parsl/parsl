import datetime
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.

if __name__ == "__main__":
    from parsl.observability.getlogs import getlogs

    log = getlogs()

    print(f"There are {len(log)} log entries")

    print("Here's a random entry:")
    print(log[random.randint(0, len(log))])

    print("unix process analysis:")
    # assumption in this comprehension that every entry has a process ID
    ps = {lr["process"] for lr in log if "process" in lr}
    print(ps)
    # assert len(ps) == 1, "assume one process in parsl.log: the submit process"

    print("task analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    ts = {(lr["parsl_task_id"], lr["parsl_dfk"]) for lr in log if "parsl_task_id" in lr}
    print(f"there are {len(ts)} tasks")
    # for t in ts:
    #  print(f"Task: {t}")

    (task_id, dfk) = (list(ts))[random.randint(0, len(ts))]

    print(f"random chosen task: {task_id}")

    print(f"=== About task {task_id}/{type(task_id)} from DFK {dfk} ===")

    tasklog = [lr for lr in log if lr.get('parsl_dfk', None) == dfk and str(lr.get('parsl_task_id', None)) == str(task_id)]
    tasklog.sort(key=lambda lr: float(lr['created']))
    for lr in tasklog:
        formatted_time = datetime.datetime.fromtimestamp(float(lr['created']))
        actor = ""
        if 'process' in lr:
            actor += lr['process']
        if 'threadName' in lr:
            action = lr['threadName'] + "@" + actor
        print(formatted_time, actor, lr['formatted'], f"({lr['logsource']})")
