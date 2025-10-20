# find common patterns in python template sequences per task/per whatever

import datetime
import random

from parsl.observability.getlogs import getlogs

if __name__ == "__main__":

    logs = getlogs()

    # group the logs by parsl_dfk, parsl_task_id and discard the rest

    log_groups = {}

    discard = 0

    for lr in logs:
        # check for 'msg' here because we specificlly only want to look for
        # templated log messages and some stuff does not have that - eg
        # RESOURCE_INFO imports (although it could do)
        if 'parsl_dfk' in lr and 'parsl_task_id' in lr \
           and 'msg' in lr and 'Standard error will' not in lr['msg'] and 'Standard out will' not in lr['msg']:
            # as an example of scriptable interactive modular analysis, filtering out those stdout/err messages would be
            # a user activity not built into the library.
            k = (lr['parsl_dfk'], lr['parsl_task_id'])
            if k not in log_groups:
                log_groups[k] = []

            # widen rather than project, as a style
            lr['analysis.key'] = lr['msg'] + "/" + lr['logsource'] + "/" + lr['threadName']

            log_groups[k].append(lr)
        else:
            discard += 1

    print(f"Grouped into {len(log_groups)} groups, with {discard} events discarded")

    print("example group:")
    k, tasklog = random.choice(list(log_groups.items()))

    tasklog.sort(key=lambda lr: lr['created'])
    for lr in tasklog:
        formatted_time = datetime.datetime.fromtimestamp(float(lr['created']))
        print(formatted_time, lr['analysis.key'])

    # new collection of tasklogs, with only msg, and sorted by msg

    sortedkeys = {}

    for k, tasklog in log_groups.items():
        # print(f"tasklog is {tasklog}")
        msgs = [lr['analysis.key'] for lr in tasklog]
        msgs.sort()
        # print(f"sorted msgs: {msgs}")

        sortedkeys[k] = tuple(msgs)

    values = [v for k, v in sortedkeys.items()]
    print(values)

    print(f"there are {len(values)} in values")

    values_set = set(values)

    print(f"there are {len(values_set)} in values_set")

    if len(values_set) != 1:
        raise RuntimeError("can't cope with different looking tasks here")

    # now work only on matching tasks, but as we assert there's only one,
    # that is all of them.

    """this is the analysis for findcommon. post grouping analysis should be modular.
    # for each task set, normalise its created time against the earliest time
    # in the task set - which if they're all in order will always be the same
    # one, but not always.

    for g in log_groups.items():
      min_created=min([float(l['created']) for l in g[1]])
      for l in g[1]:
          l['analysis_reltime'] = float(l['created']) - min_created
          print(f"annotated with reltime {l['analysis_reltime']}")
      del l

    synthetic_span = []
    for v in list(values_set)[0]:
      print(f"Value: {v}")
      # now do some statistics on timing to generated a synthetic trace that represents all the others
      # i can think of various ways this could be structured.
      # but this implementation will be the mean of relative times in each task span
      data = []
      for k, ls in log_groups.items():
          selected_ls = [l for l in ls if l['analysis.key'] == v]
          k = selected_ls[0]
          # discard any subsequent ones - e.g. stdout path info.
          # bug: the logs are not necessarily sorted so this is not necessarily picking the first one.
          # raise RuntimeError(f"selected {k} for {v}")
          rt = k['analysis_reltime']
          data.append(rt)
          del rt
          del selected_ls
      del k
      del ls
      # print(f"synthetic offset: {data}")
      print(f"mean for {v} is {sum(data)/len(data)}")
      synthetic_span.append((v, sum(data) / len(data)))
      del data
    del v

    synthetic_span.sort(key=lambda kt: kt[1])

    for k, t in synthetic_span:
        print(f"{t}: {k}")
    """
    # unlike findcommon, need to go over groupwise.

    histo = {}

    for v in list(values_set)[0]:
        histo[v] = []

    for k, ls in log_groups.items():
        print(f"log group has {len(ls)} items")
        ls.sort(key=lambda lr: float(lr['created']))
        print(f"====== task key: {k}")
        for pos in range(0, len(ls) - 1):
            log_here = ls[pos]
            log_next = ls[pos + 1]
            key = log_here['analysis.key']

            t_in_state = float(log_next['created']) - float(log_here['created'])
            print(f"{t_in_state} {log_here['analysis.key']}")

            histo[key].append(t_in_state)

    # now we have numerical histograms for each state time.
    # plot something

    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages

    # sz = math.ceil(math.sqrt(len(v)))
    # fig, axs = plt.subplots(nrows=sz, ncols=sz)

    # x = 0
    # y = 0
    with PdfPages('graphcommon.pdf') as pdf:
        for v in list(values_set)[0]:  # TODO: order by "time", eg findcommon-mean-time?
            fig, ax = plt.subplots()
            ax.hist(histo[v], bins=100)
            ax.set_title(v)
            pdf.savefig()
            plt.close()

        # x+=1
        # if x >= sz:
        #   x = 0
        #   y += 1
