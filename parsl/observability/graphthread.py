import json
import matplotlib.pyplot as plt
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
    ps = {l["process"] for l in log}
    print(ps)
    # assert len(ps) == 1, "assume one process in parsl.log: the submit process"

    # TODO: this needs to disambigute by process too, now that multiple logs are
    # imported.
    print("thread analysis:")
    # assumption in this comprehension that every entry has a thread ID
    # assumption that thread name does not change. a characteristic of denormalised
    #    wide storage: thread=>threadName but that isn't represented in the schema.
    # note that process ID isn't: temporally unique; unique at same time across hosts
    # but thats pragmatically what we have to work with right now: further work could
    # add fully unique process UUIDs but thats not what I'm working on right now.
    ts = {(l["process"], l["thread"], l["threadName"]) for l in log}
    print(f"there are {len(ts)} threads")
    # in pytest run, there are 11. more than I expected.
    for t in ts:
      print(f"Thread: {t}")

    fig, ax = plt.subplots()

    xdata = []
    ydata = []
    c = 0
    for t in ts:
        process_id = t[0]
        thread_id = t[1]
        events = [float(l["created"]) for l in log if l["thread"] == thread_id and l["process"] == process_id]
        xdata.extend(events)
        ydata.extend([t[0] + "/" + t[2] for _ in range(0,len(events))])
       
        c += 1

    assert len(xdata) == len(ydata)

    ax.scatter(x=xdata, y=ydata)

    plt.show()
