import datetime
import json
import matplotlib.pyplot as plt
import os
import random

# read in json parsl.log
# and plot something very simple

# for example, raw log entries vs time.


# TODO: note the different in return/edit semantics of
# widen_one vs widen_dict. for historical reasons, not
# because its a good idea. one returns new log records,
# the other modifies existing log records.

def widen_one(log: list, key: str, val: object) -> list:
    return [d | {key: val} for d in log]


def widen_dict(log: list, d: dict) -> None:
    for l in log:
        l.update(d)

def widen_dict_without_overwrite(log: list, d: dict) -> None:
    """Like widen_dict but raises an error if any key would be overwritten.
    A key can be updated by the dict to the same value without error.
    """
    for l in log:
        for k, v in d.items():
            assert k not in l or l[k] == v, f"Key {k} tries to update {l[k]} to {v}"
            l[k] = v


def load_jsons(path: str) -> list:
    with open(path, "r") as f:
        return [json.loads(l) for l in f.readlines()]


def get_unique(logs, key) -> object:
    """Returns the unique value of key across all log lines.

    For example, a run ID that applies to the whole log file and might be
    mentioned in several/many log records.

    Raise an error if the key does not have a unique value, because that
    means there is probably a misassumption by the caller about what the
    data looks like.
    """
    vs = {l[key] for l in logs
                 if key in l}
    assert len(vs) == 1, f"Supposedly unique key {key} has multiple values: {vs}"
    return list(vs)[0]



basedir = "runinfo/000"
# basedir = "pytest-parsl/parsltest-current/runinfo/000"
executor_dir = "htex_Local" # BAAAAD there can be many and they should be auto-discovered

def getlogs():

    log = []

    plog = load_jsons(f"{basedir}/parsl.log")

    log.extend(widen_one(plog, "logsource", "parsl.log"))

    # elog is executor logs all together
    elog = []

    # ilog is interchange log
    ilog = load_jsons(f"{basedir}/{executor_dir}/interchange.log")
    elog.extend(widen_one(ilog, "logsource", "interchange log"))
    del ilog

    # there's one interchange log above, but potentially many many manager/worker logs.
    # htex-specific tree walk here to load and annotate them all.

    # per-block:  pytest-parsl/parsltest-current/runinfo/000/htex_Local/

    des = os.scandir(f"{basedir}/{executor_dir}/")
    blockdirs = [d.name for d in des if d.name.startswith("block-")]

    # everything now should be annotated with a block_id and an executor id:
    # dfk/executor label/block ID is a globally unique block ID

    print(f"blockdirs are: {blockdirs}") 

    for b in blockdirs: 
        block_id = b[len("block-"):]
        block_extra = {"block_id": block_id, "parsl_executor": executor_dir}
        # inside the block-N directory, we should expect to find
        # one directory per manager. This manager ID is probably also asserted
        # in the manager logs, and we can perform an assert that they are
        # consistent
        mdes = os.scandir(f"{basedir}/{executor_dir}/{b}")
        managerdirs = [d.name for d in mdes]
        for m in managerdirs:
            mbytes = m.encode()  # manager_ids are bytes, not strings, in logging
            manager_extra = {"manager_id": mbytes}
            manager_dir_path = f"{basedir}/{executor_dir}/{b}/{m}"
            # in manager_dir_path should expect:
            #     manager.log
            #     worker_N.log
            # first lets import only the manager log - it has some htex task ID tagged events.
            mlog = load_jsons(f"{manager_dir_path}/manager.log")

            mlog = widen_one(mlog, "logsource", "Pool manager log")

            widen_dict(mlog, manager_extra)
            widen_dict(mlog, block_extra)

            # now look for the worker logs

            elog.extend(mlog)
            del mlog

            pooldes = os.scandir(manager_dir_path)
            worker_log_fns = [d.name for d in pooldes if d.name.startswith("worker_")]
            for w in worker_log_fns:
                worker_id = w[len("worker_"):][:-4]
                worker_extra={"worker_id": worker_id}
                # this will also be asserted in the log file in some places and the update
                # should assert that they're the same.
                wlog = load_jsons(f"{manager_dir_path}/{w}")
                wlog = widen_one(wlog, "logsource", "Pool worker log")
                widen_dict(wlog, worker_extra)
                widen_dict(wlog, manager_extra)
                widen_dict(wlog, block_extra)
                elog.extend(wlog)
 

    # complicated task joins here
    # this join also needs to happen on the plog. not only the imported executor logs.
    # so TODO move the join around wrt what it is joining over.

    # assert one DFK - because thats how we know theres the right DFK for now.
    # later, this should be a parsl.log "related log" relation
    dfk = get_unique(plog, "parsl_dfk")

    elog = widen_one(elog, "parsl_executor", executor_dir)
    elog = widen_one(elog, "parsl_dfk", dfk)
    log.extend(elog)

    # next do a JOIN-like relational activity:
    # for subset:
    # parsl_dfk, parsl_task_id, parsl_try_id, parsl_executor

    # parsl_dfk, parsl_executor, htex_task_id => parsl_task_id, parsl_try_id
    # but not vice versa: every htex task (when running under real Parsl)
    # is a parsl task, but not every parsl task runs inside this particular
    # executor.

    # every entry with an htex_task_id should be widened to include the
    # relevant parsl_task_id and parsl_try_id.
    # there is no join style left/right here, I think: the whole thing
    # operates on a single collection of logs for both the join info
    # and the output.

    # rbinds is the implication table for the above implication.

    # htex_task_id has two names, in different places:
    # on the DFK side, it is logged as parsl_executor/parsl_executor_id
    # and on the HTEX side it is htex_task_id.
    # TODO: maybe there should be a relabelling-widening here to get the
    # names the same?
    # TODO: don't separately use plog and elog: work on *one* pool of
    # logs. Because some plog lines need joining too.

    rbinds = {}
    for l in log:
        if l.get("parsl_dfk", None) == dfk and l.get("parsl_executor") == executor_dir and "parsl_task_id" in l and "parsl_executor_id" in l and "parsl_try_id" in l:
            print(f"found a binding, between parsl task {l['parsl_task_id']} try {l['parsl_try_id']} and executor task {l['parsl_executor_id']}")

            assert l['parsl_executor_id'] not in rbinds, "An executor task cannot belong to multiple tries"
            rbinds[l['parsl_executor_id']] = {'parsl_task_id': l['parsl_task_id'],
                                              'parsl_try_id': l['parsl_try_id']
                                             }

    print(f"There are {len(rbinds)} join bindings")
    # TODO: assert on various uniqueness rules of rbinds?

    for l in log:
        if "htex_task_id" in l and l['htex_task_id'] in rbinds:
            # TODO: need to match on executor label too, not only htex_task_id
            #       which means we need that executor label to exist after
            #       htex log import above. this above `if` is not a full hierarcial join key
            # TODO: assert various bad things are not happening - overwrites?
            #       as part of widen_dict_without_overwrite
            l.update(rbinds[l['htex_task_id']])


    monlog = load_parsl_monitoring(f"{basedir}/monitoring.json")

    log.extend(monlog)

    print(f"There are {len(log)} log entries") 

    return log


def load_parsl_monitoring(path):
    monlog = load_jsons(f"{basedir}/monitoring.json")
    monlog = widen_one(monlog, "logsource", "Parsl monitoring")

    # this is a lambda-widen
    for j in monlog:
       if 'first_msg' in j and j['first_msg'] == True:
           j["msg"] = "monitoring first message"
       if 'last_msg' in j and j['last_msg'] == True:
           j["msg"] = "monitoring last message"

       if 'last_msg' in j and j['last_msg'] == True and 'first_msg' in j and j['first_msg'] == True:
           raise RuntimeError(f"first last consistency error for {j}")
    return monlog
