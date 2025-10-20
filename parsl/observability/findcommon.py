# find common patterns in python template sequences per task/per whatever

import datetime
import random

from parsl.observability.getlogs import getlogs

if __name__ == "__main__":

    logs = getlogs()

    # this is a rewrite not a widen, but similar vibe
    # like a field lambda rewrite. data model wise, should be pushing more
    # automatically on this field having a consistent type (and fields in
    # general having a consistent type)
    # some sources are deserialised to python in, some to python str. 
    # its an "identifier" with a consistent string repr, so forcing to
    # string should be ok - forcing to int would also be ok.
    for l in logs:
        if 'parsl_task_id' in l:
            l['parsl_task_id'] = str(l['parsl_task_id'])


    # group the logs by parsl_dfk, parsl_task_id and discard the rest

    log_groups = {} 

    discard = 0

    for l in logs:
        # check for 'msg' here because we specificlly only want to look for
        # templated log messages and some stuff does not have that - eg
        # RESOURCE_INFO imports (although it could do)
        if 'parsl_dfk' in l and 'parsl_task_id' in l and 'msg' in l:
            k = (l['parsl_dfk'], l['parsl_task_id'])
            print(f"key is {k}")
            if k not in log_groups:
                log_groups[k] = []

            assert 'logsource' in l, f"missing logsource: {l}"
            # widen rather than project, as a style
            # and this should be done separate from grouping - its a lambda-widen, i think?
            l['analysis.message_key'] = l['msg'] + "/" + l['logsource'] + "/" + l.get('threadName', "no thread")

            log_groups[k].append(l)
            print(f"k={k}")
        else:
            discard += 1

    print(f"Grouped into {len(log_groups)} groups, with {discard} events discarded")

    # new collection of tasklogs, with only msg, and sorted by msg

    sortedkeys = {}

    for k, tasklog in log_groups.items():
        msgs = [l['analysis.message_key'] for l in tasklog]
        msgs.sort()
        sortedkeys[k] = tuple(msgs)

    values = [v for k,v in sortedkeys.items()]

    print(f"there are {len(values)} in values")

    values_set = set(values)
   
    print(f"there are {len(values_set)} in values_set")

    # print(values_set)
 
    if len(values_set) != 1:
        for v in list(values_set):
            print(v)
        raise RuntimeError("can't cope with different looking tasks here")

    # now work only on matching tasks, but as we assert there's only one,
    # that is all of them.

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
          selected_ls = [l for l in ls if l['analysis.message_key'] == v]
          k = selected_ls[0]  # discard any subsequent ones - e.g. stdout path info. bug: the logs are not necessarily sorted so this is not necessarily picking the first one.
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
