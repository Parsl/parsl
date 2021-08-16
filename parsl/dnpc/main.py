import logging
import os
import re
import sqlite3
import matplotlib.pyplot as plt

from parsl.log_utils import set_stream_logger
from typing import Dict, List

logger = logging.getLogger("parsl.dnpc.main")  # __name__ is not package qualified in __main__


class Event:
    """An Event in a context. This is deliberately minimal.
    The new state is represented as a string, which should make
    sense wrt the other states in this context (and perhaps
    with other unspecified contexts - for example, all task
    contexts should use the same set of states)

    Tooling should not expect the lists of states to be
    defined.

    It might be useful to include a human readable event provenance
    (eg the name of a log file and the line number in that log file)
    to lead users to information about a particular event.

    Time is specified as a unix timestamp. I'm unclear what the
    best representation for time is in this use case, so picking this
    fairly arbitrarily. Of some concern is that timestamps will come
    from multiple different clocks, and those clocks might need
    to be represented in the timestamp (eg with a hostname?) if use
    can be made of that information.
    """

    time: int
    type: str

    def __repr__(self):
        return f"<Event: type={self.type} time={self.time}>"


class Context:
    """Python representation of a DNPC context.

    A Context has a brief human readable name. This name should
    make sense within the containing context, and should be sized to
    be useful as (for example) a graph label. For example "Task 23"
    It might be, given the type field, that the name only needs to
    make sense alongside the type (so name could be "23" if type
    is "parsl.task")

    A context may contain subcontexts.

    A context may be contained in many supercontexts - a context does
    not know about and does not have an enclosing supercontext.

    The object representation stores the containment
    arrows only from super- to sub- contexts.

    A context may content events / state transitions (I'm unclear on the
    vocabulary I want to use there, and on exactly what should be
    represented.

    The type str indicates the kind of events/subcontexts one might expect
    to find inside a context, and indicates which contexts might be
    compared to each other - eg all task contexts in some sense look the same.
    This is, however, schema and definition free. My recommendation is
    to use some name like "parsl.subcomponent.whatever"

    The subcontexts collection should not be directly set or edited - it should
    be maintained by helper methods provided as part of the Context
    implementation.

    A user should not call the Context() constructor directly - instead use
    the new_root_context and get() class methods


    """
    type: str
    name: str
    _subcontexts: Dict[str, "Context"]
    events: List[Event]

    def __init__(self):
        self._subcontexts = {}
        self.events = []
        self.name = "unnamed"

    def __repr__(self):
        return f"<Context {self.type} ({self.name}) with {len(self._subcontexts)} subcontexts, {len(self.events)} events>"

    @classmethod
    def new_root_context(cls):
        return Context()

    #    context = root_context.get_context("monitoring", "parsl.monitoring.db")
    def get_context(self, edge_name, type):
        edge_name = str(edge_name)
        c = self._subcontexts.get(edge_name)
        if c is not None:
            assert(c.type == type)
            logger.info(f"get_context returning existing {type} context for key {edge_name}")
            return c
        else:
            c = Context()
            c.type = type
            self._subcontexts[edge_name] = c
            logger.info(f"get_context creating new {type} context for key {edge_name}")

            return c

    def alias_context(self, edge_name: str, context: "Context"):
        c = self._subcontexts.get(edge_name)
        if c is not None:
            assert c is context  # object, not value, identity
        else:
            self._subcontexts[edge_name] = context

    @property
    def subcontexts(self) -> List["Context"]:
        """The subcontexts property is read-only. It should be maintained by
        Context helper methods."""
        return [self._subcontexts[k] for k in self._subcontexts]


def import_workflow_task_tries(base_context: Context, db: sqlite3.Connection, run_id: str, task_id) -> None:
    logger.info(f"Importing tries for task {task_id}")

    cur = db.cursor()

    # this fractional seconds replacement for %s comes from (julianday('now') - 2440587.5)*86400.0
    # SELECT (julianday('now') - 2440587.5)*86400.0;

    for row in cur.execute(f"SELECT try_id, (julianday(task_try_time_launched) - 2440587.5)*86400.0, "
                           f"(julianday(task_try_time_running) - 2440587.5)*86400.0, (julianday(task_try_time_returned) - 2440587.5)*86400.0 "
                           f"FROM try WHERE run_id = '{run_id}' AND task_id = '{task_id}'"):
        try_id = row[0]

        try_context = base_context.get_context(try_id, "parsl.try")
        try_context.name = "Try {try_id}"

        if row[1]:  # omit this event if it is NULL
            launched_event = Event()
            launched_event.type = "launched"
            launched_event.time = float(row[1])
            try_context.events.append(launched_event)

        if row[2]:  # omit this event if it is NULL
            running_event = Event()
            running_event.type = "running"
            running_event.time = float(row[2])
            try_context.events.append(running_event)

        returned_event = Event()
        returned_event.type = "returned"
        returned_event.time = float(row[3])
        try_context.events.append(returned_event)

    return None


def import_workflow_tasks(base_context: Context, db: sqlite3.Connection, run_id: str) -> None:
    logger.info(f"Importing tasks for workflow {run_id}")

    cur = db.cursor()

    for row in cur.execute(f"SELECT task_id, strftime('%s', task_time_invoked), strftime('%s',task_time_returned) FROM task WHERE run_id = '{run_id}'"):
        task_id = row[0]
        task_context = base_context.get_context(task_id, "parsl.task")
        task_context.name = f"Task {task_id}"

        summary_context = task_context.get_context("summary", "parsl.task.summary")
        summary_context.name = f"Task {task_id} summary"

        start_event = Event()
        start_event.type = "start"
        start_event.time = float(row[1])
        summary_context.events.append(start_event)

        end_event = Event()
        end_event.type = "end"
        end_event.time = float(row[2])
        summary_context.events.append(end_event)

        state_context = task_context.get_context("states", "parsl.task.states")
        state_context.name = f"Task {task_id} states"

        state_cur = db.cursor()
        for state_row in state_cur.execute(f"SELECT task_status_name, (julianday(timestamp) - 2440587.5)*86400.0 "
                                           f"FROM status WHERE run_id = '{run_id}' AND task_id = '{task_id}'"):
            start_event = Event()
            start_event.type = state_row[0]
            start_event.time = float(state_row[1])
            state_context.events.append(start_event)

        import_workflow_task_tries(task_context, db, run_id, task_id)

    return None


def import_parsl_log(base_context: Context, rundir: str) -> None:
    logger.info("Importing parsl.log")

    with open(f"{rundir}/parsl.log", "r") as logfile:
        for line in logfile:
            # the key lines i want for now from parsl.log look like this:
            # Parsl task 562 try 0 launched on executor WorkQueueExecutor with executor id 337
            m = re.match('.* Parsl task (.*) try (.*) launched on executor (.*) with executor id (.*)', line)
            if m:
                logger.info(f"Line matched: {line}, {m}")
                task_id = m.group(1)
                logger.info(f"Task ID {task_id}")
                task_context = base_context.get_context(task_id, "parsl.task")
                try_id = m.group(2)
                logger.info(f"Try ID {try_id}")
                try_context = task_context.get_context(try_id, "parsl.try")
                executor_id_context = try_context.get_context("executor", "parsl.try.executor")
                # the point of this log file line is to alias it
                # separate importing of executor-specific log file will populate
                # the parsl.try.executor context via the below aliased context
                executor_name = m.group(3)
                executor_id = m.group(4)
                executor_context = base_context.get_context(executor_name, "parsl.executor")
                executor_context.alias_context(executor_id, executor_id_context)

    logger.info("Finished importing parsl.log")


def import_work_queue_python_timing_log(base_context: Context, rundir: str):
    # These logs (like the workqueue results files) aren't scoped properly
    # by executor - if there were two work queue executors in a run they
    # would conflict.
    wq_context = base_context.get_context("WorkQueueExecutor", "parsl.executor")
    dirs = os.listdir(f"{rundir}/function_data/")
    for dir in dirs:
        wqe_task_id = str(int(dir))  # normalise away any leading zeros
        wq_task_context = wq_context.get_context(wqe_task_id, "parsl.try.executor")
        epf_context = wq_task_context.get_context("epf", "parsl.wq.exec_parsl_function")
        # now import the log_file into epf_context
        filename = f"{rundir}/function_data/{dir}/log"
        if os.path.exists(filename):
            with open(filename) as f:
                for line in f:
                    # 1629049247.4333403 LOADFUNCTION
                    m = re.match('^([0-9\\.]+) ([^ ]+)\n$', line)
                    if m:
                        event = Event()
                        event.time = float(m.group(1))
                        event.type = m.group(2)
                        epf_context.events.append(event)


def import_work_queue_transaction_log(base_context, rundir):
    # TODO: how to determine if we should import this log? should it be
    # triggered by an entry in the parsl.log file that declares that a
    # WQ executor exists?
    # for now doing my testing, I'll assume that there will be a log in the
    # WorkQueueExecutor/ subdirectory

    wq_context = base_context.get_context("WorkQueueExecutor", "parsl.executor")

    logger.info("Importing Work Queue transaction log")
    with open(f"{rundir}/WorkQueueExecutor/transaction_log") as transaction_log:
        for line in transaction_log:
            m = re.match('([0-9]+) [0-9]+ TASK ([0-9]+) ([^ ]+) .*', line)
            if m:
                logger.info(f"Line matched: {line}, {m}")
                task_id = m.group(2)
                status = m.group(3)
                logger.info(f"WQ task {task_id} status {status}")
                wq_task_context = wq_context.get_context(task_id, "parsl.try.executor")
                event = Event()
                event.time = float(m.group(1)) / 1000000
                event.type = status
                wq_task_context.events.append(event)

    logger.info("Done importing Work Queue transaction log")


def import_parsl_rundir(base_context: Context, rundir: str) -> None:
    logger.info(f"Importing rundir {rundir}")

    # things we might find in the rundir:

    # almost definitely parsl.log - this has lots of task timing info in it,
    # a third source of task times distinct from the two monitoring db times.
    # It also has bindings between task IDs and executor IDs, and in the
    # workqueue case, bindings between wq-executor ID and work queue IDs.
    # The task timing info might be interesting for when people aren't using
    # the monitoring db, although the broad story at the moment should probably
    # still be that if you want to analyse parsl-level task timings, use the
    # monitoring db.

    import_parsl_log(base_context, rundir)
    import_work_queue_transaction_log(base_context, rundir)
    import_work_queue_python_timing_log(base_context, rundir)

    # workqueue debug log - this is what I'm most interested in integrating
    # alongside the monitoring db as it will link parsl monitoring DB state
    # transitions with WQ level transitions.

    logger.info(f"Finished importing rundir {rundir}")


def import_workflow(base_context: Context, db: sqlite3.Connection, run_id: str) -> None:
    logger.info(f"Importing workflow {run_id}")

    context = base_context.get_context(run_id, "parsl.workflow")

    cur = db.cursor()

    rundir = None

    # TODO: sql injection protection (from eg hostile user sending hostile db - run_id is not sanitised)
    for row in cur.execute(f"SELECT strftime('%s', time_began), strftime('%s',time_completed), rundir FROM workflow WHERE run_id = '{run_id}'"):
        # in a well formed DB will iterate only once

        start_event = Event()
        start_event.type = "start"
        start_event.time = float(row[0])
        context.events.append(start_event)

        end_event = Event()
        end_event.type = "end"
        end_event.time = float(row[1])
        context.events.append(end_event)

        rundir = row[2]
        # TODO: we'll get the last rundir silently discarding
        # others if there are multiple workflows with the same ID
        # rather than giving an error...

    import_workflow_tasks(context, db, run_id)

    # there are also things defined in the parsl log (indeed, a decent amount
    # of information could come from the parsl.log file without any
    # monitoring.db at all - and maybe that's an interesting mode to support...)

    import_parsl_rundir(context, rundir)

    # c2 = import_workflow_parsl_log(context, run_id, rundir)

    # TODO: a heirarchy merge operator that lets c2 be overlaid on top of
    # the existing context. This means that the import_workflow_parsl_log
    # importer does not need an existing monitoring.db based context graph
    # to already exist - meaning it should be more amenable to use on files
    # without the monitoring db.
    # However, it then needs a notion of identity between the trees, which
    # is not implemented at the moment: how much should that identity
    # structure be baked into the code rather than specified as part of the
    # merge? This is similar to a JOIN operation, but deeply heirarchical...
    # There's also the question of context identity: previously a python
    # Context object was a context: object identity was context identity,
    # which I intended to use for expressing DAGs, by using DAGs of objects.
    # This "merge" operator gets rid of that: two Context objects (which may
    # be referred to in complicated fashions elsewhere) now need to become
    # one Context object (either re-using one of the exising ones or
    # a third new one).
    # If we're specifying keys, we're getting a bit schema-ey. But specifying
    # join keys as part of the JOIN / merge makes sense if it looks like
    # SQL style JOINs, where the fields to join on are specified as part of
    # the JOIN, not as part of the schema.
    # A different database-like approach is rather than ever calling
    # the Context constructor directly, there is a "context.declare_or_find(key)"
    # (or more simply phrased context.subcontext(type, key))
    # call which allows either the existing keyed context or a new one if
    # it does not exist - to be accessed, and modified. In that way, the
    # Context objects remain unique to their keys. And the database consists
    # of an incrementally appended collection of contexts - an importer may
    # add subcontexts to any existing context.
    # This means there should be keys in the formal query model - either
    # on contexts or on the context/subcontext edge - I don't have a feel
    # for which is better - probably on the edge, because keys make sense
    # in a context, and subcontexts can be in many contexts. Eg a try with
    # key 0 makes sense in a context of a task key n in workflow key uuuuu,
    # but doesn't in a collection of tries from many tasks, where they might
    # instead be keyed by executor job id (or even unkeyed)

    logger.info(f"Done importing workflow {run_id}")
    return context


def import_monitoring_db(root_context: Context, dbname: str) -> Context:
    """This will import an entire monitoring database as a context.
    A monitoring database root context does not contain any events
    directly; it contains each workflow run as a subcontext.
    """
    logger.info("Importing context from monitoring db")
    context = root_context.get_context("monitoring", "parsl.monitoring.db")
    context.type = "parsl.monitoring.db"
    context.name = "Parsl monitoring database " + dbname

    # TODO: can this become a with: ?
    db = sqlite3.connect(dbname,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    # create a subcontext for each workflow row

    cur = db.cursor()

    for row in cur.execute("SELECT run_id FROM workflow"):
        run_id = row[0]
        logger.info(f"workflow: {run_id}")

        import_workflow(context, db, run_id)

    db.close()

    logger.info("Finished importing context from monitoring db")
    return context


def plot_wq_running_to_parsl_running_histo(db_context):

    all_try_contexts = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            logger.info(f"task subcontexts have keys: {task_context._subcontexts.keys()}")
            try_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.try']
            all_try_contexts += try_contexts

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        logger.info(f"examining try context {context}")
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        executor_contexts = [c for c in context.subcontexts if c.type == 'parsl.try.executor']
        logger.info(f"context.subcontexts = {context.subcontexts}")
        logger.info(f"executor_contexts = {executor_contexts}")
        if len(executor_contexts) != 1:
            logger.info("skipping because wrong number of executor_contexts")
            continue
        pte_context = executor_contexts[0]

        pte_event_types = set()
        for event in pte_context.events:
            pte_event_types.add(event.type)

        logger.info(f"event_types: {event_types}")
        logger.info(f"pte_event_types: {pte_event_types}")

        if "running" in event_types and 'RUNNING' in pte_event_types:
            filtered_try_contexts.append(context)

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        parsl_running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        executor_contexts = [c for c in context.subcontexts if c.type == 'parsl.try.executor']
        logger.info(f"executor_contexts = {executor_contexts}")
        assert(len(executor_contexts) == 1)
        pte_context = executor_contexts[0]

        wq_running_events = [e for e in pte_context.events if e.type == "RUNNING"]
        wq_running_event = wq_running_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = parsl_running_event.time - wq_running_event.time

        xs.append(runtime)

    logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("time from wq running to parsl running histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-wq-running-to_parsl-running-histo.png")


def plot_tries_runtime_histo(db_context):

    all_try_contexts = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            try_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.try']
            all_try_contexts += try_contexts

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        if "running" in event_types and "returned" in event_types:
            filtered_try_contexts.append(context)

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        returned_events = [e for e in context.events if e.type == "returned"]
        returned_event = returned_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = returned_event.time - running_event.time

        xs.append(runtime)

    logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("try runtime histogram")

    ax.hist(xs)

    plt.savefig("dnpc-tries-runtime-histo.png")


def plot_tries_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tries of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            try_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.try']
            for try_subcontext in try_contexts:
                all_subcontext_events += try_subcontext.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tries-cumul.png")


def plot_tasks_summary_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            state_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.task.summary']
            for task_subcontext in state_contexts:
                all_subcontext_events += task_subcontext.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tasks-summary-cumul.png")


def plot_tasks_status_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            state_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.task.states']
            for task_subcontext in state_contexts:
                all_subcontext_events += task_subcontext.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tasks-status-cumul.png")


def plot_tasks_status_streamgraph(db_context):

    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            state_contexts = [sc for sc in task_context.subcontexts if sc.type == 'parsl.task.states']
            all_state_subcontexts.update(state_contexts)

    plot_context_streamgraph(all_state_subcontexts, "dnpc-tasks-status-stream.png")


def plot_task_running_event_streamgraph(db_context):
    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            this_task_contexts = set()
            # this_task_contexts.add(task_context)
            try_contexts = [tc for tc in task_context.subcontexts if tc.type == 'parsl.try']
            # this_task_contexts.update(try_contexts)
            for try_subcontext in try_contexts:
                wq_contexts = [tc for tc in try_subcontext.subcontexts if tc.type == 'parsl.try.executor']
                this_task_contexts.update(wq_contexts)
                for wq_subcontext in wq_contexts:
                    all_state_subcontexts.update(wq_subcontext.subcontexts)

            state_contexts = [tc for tc in task_context.subcontexts if tc.type == 'parsl.task.states']
            this_task_contexts.update(state_contexts)
            collapsed_context = Context.new_root_context()
            for c in this_task_contexts:
                collapsed_context.events += c.events
            collapsed_context.events.sort(key=lambda e: e.time)

            end_states = ['pending', 'launched', 'WAITING', 'exec_done', 'failed', 'memo_done', 'dep_fail', 'DONE']
            all_state_subcontexts.add(collapsed_context)

    plot_context_streamgraph(all_state_subcontexts, "dnpc-tasks-running-event-stream.png", hide_states=end_states)


def plot_context_streamgraph(all_state_subcontexts, filename, hide_states=[]):

    all_subcontext_events = []

    for context in all_state_subcontexts:
        all_subcontext_events += context.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    # now generate a different stream of events, to be used for plotting:
    # for each task,
    # the first event increases the event type
    # subsequent events increase the event type and decrease the former
    # event type

    plot_events = {}
    for t in event_types:
        plot_events[t] = []

    for s in all_state_subcontexts:
        if len(s.events) == 0:
            continue

        these_events = [e for e in s.events]  # copy so we can mutate safely
        these_events.sort(key=lambda e: e.time)

        plot_events[these_events[0].type].append((these_events[0].time, 1))
        prev_event_type = these_events[0].type
        for e in these_events[1:]:
            plot_events[e.type].append((e.time, 1))
            plot_events[prev_event_type].append((e.time, -1))
            prev_event_type = e.type
        # if prev_event_type != "exec_done":
        #    raise RuntimeError(f"did not end on exec_done: {prev_event_type}, {these_events}")

    # TODO: now we have per-event type data series, with mismatching x axes
    # for each of those data series, align the x axes by duplicating entries
    # to ensure the x axis is fully populated

    canonical_x_axis_set = set()
    for t in event_types:
        these_x = [e[0] for e in plot_events[t]]
        logger.info(f"these_x = {these_x}")
        logger.info(f"event type {t} adding {len(these_x)} timestamps")
        logger.info(f"size before update: {len(canonical_x_axis_set)}")
        canonical_x_axis_set.update(these_x)
        logger.info(f"size after update: {len(canonical_x_axis_set)}")

    canonical_x_axis = list(canonical_x_axis_set)
    canonical_x_axis.sort()

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(1, 1, 1)

    ys = []
    labels = []

    for event_type in event_types:

        y = []
        these_events = plot_events[event_type]

        these_events.sort(key=lambda pe: pe[0])

        n = 0
        for x in canonical_x_axis:

            while len(these_events) > 0 and these_events[0][0] == x:
                assert these_events[0][0] in canonical_x_axis_set, "timestamp must be in x axis somewhere"
                assert these_events[0][0] in canonical_x_axis, "timestamp must be in x axis list somewhere"
                n += these_events[0][1]
                these_events = these_events[1:]

            assert len(these_events) == 0 or these_events[0][0] > x, "Next event must be in future"
            y.append(n)

        # we should have used up all of the events for this event type
        assert these_events == [], f"Some events remaining: {these_events}"

        logger.info(f"will plot event {event_type} with x={x} and y={y}")

        if event_type not in hide_states:
            ys.append(y)
            labels.append(event_type)

    ax.stackplot(canonical_x_axis, ys, labels=labels, baseline='wiggle')
    ax.legend(loc='upper left')
    plt.title("tasks in each state by time")

    plt.savefig(filename)


def plot_all_task_events_cumul(db_context, filename="dnpc-all-task-events-cumul.png"):
    all_subcontext_events = []

    # TODO: this should maybe use a set for all_subcontext_events:
    # in some cases, there might be multiple routes to the same context,
    # and each context should only be counted once.
    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            all_subcontext_events += task_context.events

            try_contexts = [tc for tc in task_context.subcontexts if tc.type == 'parsl.try']
            for try_subcontext in try_contexts:
                all_subcontext_events += try_subcontext.events
                wq_contexts = [tc for tc in try_subcontext.subcontexts if tc.type == 'parsl.try.executor']
                for wq_subcontext in wq_contexts:
                    all_subcontext_events += wq_subcontext.events
                    for s in wq_subcontext.subcontexts:
                        all_subcontext_events += s.events

            state_contexts = [tc for tc in task_context.subcontexts if tc.type == 'parsl.task.states']
            for state_context in state_contexts:
                all_subcontext_events += state_context.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative task events (parsl/wq/worker) by time")

    plt.savefig(filename)


def plot_wq_parsl_worker_cumul(db_context):

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts:
        task_contexts = [sc for sc in wf_context.subcontexts if sc.type == 'parsl.task']
        for task_context in task_contexts:
            try_contexts = [tc for tc in task_context.subcontexts if tc.type == 'parsl.try']
            for try_subcontext in try_contexts:
                wq_contexts = [tc for tc in try_subcontext.subcontexts if tc.type == 'parsl.try.executor']
                for wq_subcontext in wq_contexts:
                    all_subcontext_events += wq_subcontext.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative wq-parsl worker events by time")

    plt.savefig("dnpc-wq-parsl-worker-cumul.png")


def plot_workflows_cumul(db_context):
    """An example of making a plot. Given a database context,
    looks at all of the contained contexts (without caring about
    type, which is probably wrong), and plots the state
    transitions for all of those immediate child contexts.
    """

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for context in db_context.subcontexts:
        all_subcontext_events += context.events

    logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db workflow events by time")

    plt.savefig("dnpc-workflows-cumul.png")


def main() -> None:
    set_stream_logger()
    logger.info("dnpc start")

    root_context = Context.new_root_context()

    import_monitoring_db(root_context, "./monitoring.db")

    monitoring_db_context = root_context.get_context("monitoring", "parsl.monitoring.db")

    logger.info(f"got monitoring db context {monitoring_db_context}")

    # now do some simple plots with this context - at time of writing
    # all that is available is workflow start/end times but that should
    # allow plots of number of workflows in each state, which is a
    # building block to later plots.

    plot_workflows_cumul(monitoring_db_context)
    plot_tasks_summary_cumul(monitoring_db_context)
    plot_tasks_status_cumul(monitoring_db_context)
    plot_tries_cumul(monitoring_db_context)
    plot_tries_runtime_histo(monitoring_db_context)
    plot_wq_running_to_parsl_running_histo(monitoring_db_context)
    plot_wq_parsl_worker_cumul(monitoring_db_context)
    plot_all_task_events_cumul(monitoring_db_context)
    plot_tasks_status_streamgraph(monitoring_db_context)
    plot_task_running_event_streamgraph(monitoring_db_context)

    logger.info("dnpc end")


if __name__ == "__main__":
    main()
