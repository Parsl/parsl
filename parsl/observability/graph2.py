import json
import matplotlib.pyplot as plt
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.


if __name__ == "__main__":

    log = []

    with open("pytest-parsl/parsltest-current/runinfo/000/parsl.log", "r") as f:
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

    print("thread analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    ts = {(l["thread"], l["threadName"]) for l in log}
    print(f"there are {len(ts)} threads")
    # in pytest run, there are 11. more than I expected.
    for t in ts:
      print(f"Thread: {t}")

    fig, ax = plt.subplots()

    xdata = []
    ydata = []
    data_names = ["task+dfk", "dfk (no task)", "not associated"]

    def level(l):
        if "parsl_dfk" and "parsl_task_id" in l:
            return 0
        elif "parsl_dfk" in l:
            return 1
        else:
            print(f"WL: {l['msg']}")
            return 2

    events = [(float(l["created"]), l) for l in log]
    xdata.extend([e[0] for e in events])
    ydata.extend([data_names[level(e[1])] for e in events])
    data_names.append(t[1])

    assert len(xdata) == len(ydata)

    ax.scatter(x=xdata, y=ydata)

    plt.show()

