import json
import os

from typing import Optional

from parsl.observability.utils import widen_one, widen_dict, widen_lambda, get_unique


def load_jsons(path: str) -> list:
    try:
        with open(path, "r") as f:
            return [json.loads(lr) for lr in f.readlines()]
    except Exception:
        print(f"exception loading from {path}")
        raise


# basedir = "runinfo/002"
# basedir = "pytest-parsl/parsltest-current/runinfo/000"
# executor_dir = "htex_Local" # BAAAAD there can be many and they should be auto-discovered


def getlogs(basedir):

    log = []

    plog = load_jsons(f"{basedir}/parsl.jsonlog")

    log.extend(widen_one(plog, "logsource", "parsl.jsonlog"))

    # elog is executor logs all together

    # TODO: names and types of executors should be computed, not
    # hard-coded here.
    ex_name = "htex_Local"
    elog = import_htex_executor(basedir, ex_name)

    # ex_name = "WorkQueueExecutor"
    # elog = import_wq_executor(basedir, ex_name)

    # assert one DFK - because thats how we know theres the right DFK for now.
    # later, this should be a parsl.jsonlog "related log" relation
    dfk = get_unique(plog, "parsl_dfk")

    elog = widen_one(elog, "parsl_executor", ex_name)
    elog = widen_one(elog, "parsl_dfk", dfk)
    log.extend(elog)

    # complicated task joins here
    # this join also needs to happen on the plog. not only the imported executor logs.
    # so TODO move the join around wrt what it is joining over.

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

    # in the presence of multiple join-likes, should be computing a fixpoint?
    # eg keep running each join repeatedly until a whole cycle adds no new
    # attributes (and so the next round also won't)?
    # that is maybe easier for understanding how widening relations propagate,
    # although slower.

    # likewise, might want to re-run/re-fixpoint if new log records are added
    # later...
    # as well as anything involving a filter or lambda-widen. which might
    # suggest recording all the widens and joins inside a query state and
    # re-running semi-automatically? or some other further embeddings of
    # queries rather than raw lists and dicts.

    # for work queue executor, we have a wq_task_id which is only bound to
    # an executor ID "later", after actual WQ submission which is separate
    # from the submit() method - actually htex has that a bit, because the
    # htex task id is not allocated in submit() any more.

    # the first join implication is that when we have a bind from
    # parsl_executor_id and wq_task_id, we should propagate the parsl_executor_id
    # onto the work queue level records.

    rbinds = {}
    for lr in log:
        if "parsl_executor_id" in lr and "wq_task_id" in lr:
            assert str(lr['wq_task_id']) not in rbinds, "A WQ task cannot belong to multiple WorkQueueExecutor tasks"
            rbinds[str(lr['wq_task_id'])] = {"parsl_executor_id": lr['parsl_executor_id']}

    print(f"There are {len(rbinds)} join bindings")

    for lr in log:
        if "wq_task_id" in lr:
            assert str(lr['wq_task_id']) in rbinds, f"Trying to bind a task that does not have join info: {repr(lr['wq_task_id'])}, {rbinds!r}"
            lr.update(rbinds[str(lr['wq_task_id'])])

    del rbinds

    rbinds = {}
    for lr in log:
        if lr.get("parsl_dfk", None) == dfk and \
           lr.get("parsl_executor") == ex_name and \
           "parsl_task_id" in lr and \
           "parsl_executor_id" in lr and \
           "parsl_try_id" in lr:
            print(f"found a binding, between parsl task {lr['parsl_task_id']} "
                  f"try {lr['parsl_try_id']} and executor task {lr['parsl_executor_id']}")

            # TODO: this needs to include some disambiguation for dfk/executor, perhaps in the rbinds key...
            # because eg. two htexes in the same run will have two executor task 1s. And I should be able
            # to test that - this assert should fail at that point.
            assert lr['parsl_executor_id'] not in rbinds, "An executor task cannot belong to multiple tries"
            rbinds[lr['parsl_executor_id']] = {'parsl_task_id': lr['parsl_task_id'],
                                               'parsl_try_id': lr['parsl_try_id']
                                               }

    print(f"There are {len(rbinds)} join bindings")
    # TODO: assert on various uniqueness rules of rbinds?

    for lr in log:
        if "parsl_executor_id" in lr:  # and l['parsl_executor_id'] in rbinds:
            assert lr['parsl_executor_id'] in rbinds   # sometimes you want an error check, sometimes a skip as above
            # TODO: need to match on executor label too, not only htex_task_id
            #       which means we need that executor label to exist after
            #       htex log import above. this above `if` is not a full hierarcial join key
            # TODO: assert various bad things are not happening - overwrites?
            #       as part of widen_dict_without_overwrite
            lr.update(rbinds[lr['parsl_executor_id']])
            # print(f"processing bind: {l}")

    # l2 = [l for l in log if 'wq_object_type' in l and l['wq_object_type'] == "TASK"]
    # print(l2[0])

    monpath = f"{basedir}/monitoring.json"
    if os.path.exists(monpath):
        monlog = load_parsl_monitoring(basedir, monpath)
        log.extend(monlog)

    print(f"There are {len(log)} log entries")

    return log


def import_htex_executor(basedir, executor_dir):

    elog = []

    # ilog is interchange log
    ilog = load_jsons(f"{basedir}/{executor_dir}/interchange.jsonlog")
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
            mlog = load_jsons(f"{manager_dir_path}/manager.jsonlog")

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
                worker_extra = {"worker_id": worker_id}
                # this will also be asserted in the log file in some places and the update
                # should assert that they're the same.
                wlog = load_jsons(f"{manager_dir_path}/{w}")
                wlog = widen_one(wlog, "logsource", "Pool worker log")
                widen_dict(wlog, worker_extra)
                widen_dict(wlog, manager_extra)
                widen_dict(wlog, block_extra)
                elog.extend(wlog)

    widen_lambda(elog, lambda lr: {"parsl_executor_id": lr['htex_task_id']} if 'htex_task_id' in lr else {})
    return elog


def parse_wq_transaction_log_line(entry: str) -> Optional[dict]:
    if entry == "\n":
        return None
    if entry.startswith("#"):
        return None
    s = entry.split(" ")
    print(s)
    created = float(s[0]) / 1000000
    manager_pid = s[1]
    object_type = s[2]
    d = {"created": created, "wq_manager_pid": manager_pid, "wq_object_type": object_type, "formatted": entry.strip()}

    if object_type == "TASK":
        d['wq_task_id'] = int(s[3])
        d['wq_task_state'] = s[4]
        d['msg'] = f"TASK {s[4]}"

    # TODO: other kinds of transaction_log object

    return d


def import_wq_executor(basedir, executor_dir) -> list[dict]:
    # there are three log files. two of them are machine readable:
    #   master_log has metrics about the system as a whole
    #   which could be imported under the executor but has no task
    #   relationship.
    #   transaction_log which has event-style information about
    #   tasks, managers, transfers.
    # For this work so far, I'm only going to import transaction_log,
    # and initially focus on transaction_log.
    path = f"{basedir}/{executor_dir}/transaction_log"
    with open(path, "r") as f:
        r = [parse_wq_transaction_log_line(lr) for lr in f.readlines()]

    r = [lr for lr in r if lr is not None]
    r = widen_one(r, "logsource", "Work Queue transaction_log")

    sw_logs = load_jsons(f"{basedir}/{executor_dir}/submitwait.jsonlog")
    sw_logs = widen_one(sw_logs, "logsource", "Work Queue submitwait.jsonlog")

    return [lr for lr in r if lr is not None] + sw_logs


def load_parsl_monitoring(basedir, path):
    monlog = load_jsons(f"{basedir}/monitoring.json")
    monlog = widen_one(monlog, "logsource", "Parsl monitoring")

    # this is a lambda-widen
    for j in monlog:
        if 'first_msg' in j and j['first_msg']:
            j["msg"] = "monitoring first message"
        if 'last_msg' in j and j['last_msg']:
            j["msg"] = "monitoring last message"

        if 'last_msg' in j and j['last_msg'] and 'first_msg' in j and j['first_msg']:
            raise RuntimeError(f"first last consistency error for {j}")
    return monlog
