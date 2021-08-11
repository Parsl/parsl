distributed nested performance contexts
=======================================

distributed tracing style stuff for parsl and related components.

distributed tracing has a single request ID that is propagated everywhere [citation]

this work is intended to make things a bit more heirarchical, where an activity
as well as having logs/states itself, also contains subactivities.

it is an exploration of *how* to express useful information, rather than an
attempt to implement it in an efficient manner - I think its likely that
with some fleshing out of how things should work, it might become apparant
that (for example) some existing graph database delivers the right
query behaviour.

see nested diagnostic context work in java [citation]

see netlogger [citation]

see graph query languages in general

see buneman - keys for XML - for some thoughts about identity for merges/joins
eg https://repository.upenn.edu/cgi/viewcontent.cgi?article=1122&context=cis_papers

the key goal for the current work is performance analysis of parsl
tasks as they are executed through the system - but including the non-core
parsl stuff: perhaps a little bit inside the tasks, definitely inside the
engines that sit alongside parsl helping the tasks run.

not-goals:

* live information integration between data sources - so components
  can dump out stuff wherever/however without time constraints. this is all
  post-hoc analysis

* instrumenting every piece of tech in the stack using the same technology,
  so custom per-component log file scraping is OK. Requiring the components to
  change to work with this logging mechanism is not a requirement (and mostly
  impossible if it's things installed on the host system rather than in a user
  environment)

vocab:
  context - a thing which has states/log lines/... across multiple log sources
      for example a parsl task
  subcontext - a context which is fully contained within another context.
      for example, a parsl ``try`` is fully contained within a parsl ``task``.

components of the system emit log-like info - logfiles, monitoring.db - which
associate *events* - eg state transitions, log lines - with a particular context.

it might be that a particlar source has a particular implicit containing
context - eg a particular logfile is only for a particular try context, which means
it is then contained in a particular task context without the log file ever
mentioning that task context.

do contexts have to have explicit IDs? maybe not - eg if there's an adhoc
context coming from a single log file.

the primary output goal for now is for all parsl tasks, to get a (fine-grained as
desired by the user) list of all state transitions / log lines that are
directly associated with that.


a particular "sub"-context may be contained within multiple parent contexts,
which suggests that having unique primary keys for a nested context is not
the right thing to do: for example, a particular try may be (mostly) contained within a worker context
(i say mostly, because some of the try happens on the submit side - which
suggests theres a worker-side try subcontext, that forms part of the main
try context:
workflow > task > try > executor-level-try > worker-side-try
workflow > executor > block > worker > worker-side-try
workflow > executor > executor-level-try

nested contexts should be cheap: easy to create by a new binding, and in the
tooling easy to ignore layer-wise - in the sense that in the above first
example, try and worker-side-try don't form heavily distinct layers in some
analyses, perhaps.

binding of contexts should be flexible to specify, in order that they can be
placed at convenient points, rather than requiring components to necessarily
know their own context (or even that they're part of nested contexts at all)

labelling of contexts should be flexible: no global unique ID should be
needed outside of analysis. identity should maybe look like "In the context of
this log file (which is known to analysis code), these new subcontexts have
these labels, and they relate to certain sub-log files in these ways"

when a context in a subcontext of two different contexts (try inside both
task and executor) then it doesn't make sense to have a single full-path
primary key globally.

Things that need to happen for parsl:

  identify what is a context, concretely, especially where its vague like
    different executors (target: local, htex, wq)

  ensure appropriate subcontext binding happens somewhere accessible

  simple analysis tool that works given monitoring.db and log files to
  determine if this is worth working on - maybe python-centric as thats
  what everyone is familiar with? and code-driven seems to be the main
  monitoring driver right now.

Main driving usecase: jim's gen3 work, wq+cori

Example of a context >= than a parsl-level workflow might be:
  * a single bps run - although that might be a one-to-one mapping
  * a campaign of runs - identified by a human with some informal name, perhaps, or a directory
  * a collection of runs described in a single monitoring database - even without any other log files at all, this is a substantial collection of information - those core parsl monitoring information.

Example of a context that is < a parsl-level task try:
  * executor-try - eg workqueue's parsl helper script
  * inside-task progress: eg starting up singularity/shifter in a shell wrapper.

Both of these seem to be potentially large users of worker time in the
DESC case, and both of these would be useful to understand.

- inside-command-line-app progress: eg jim has been pulling out info from the app log files that might be of interest to represent.



identities:
nodes might have an intrinsic ID - eg a workflow knows its own run_id
but they might also be identified by a label on an edge - eg a slurm job
does not know its own parsl-level block ID - or even that it is a
parsl block at all.

principle:
there is no canonical source of information about anything (hence the graph
merge requirements) - eg multiple entities assert that workflow X has
task N. (eg monitoring.db, parsl.log) and neither is more authentic than the
other.

principle:
components are necessarily aware of each other, nor bound in a strict
hierarchy

the stack is composed (aka configured) by the workflow author/user, and so
the performance analysis stack is also composed (aka configured)
correspondingly.

expect to be doing ad-hoc workflow and query aware remapping of contexts and
events

expect dirty data that doesn't always align quite right: eg three different
components might all give their own "end" event with very slightly different
timing, and not always in the same order - that's part of what I mean by
"distributed".

components not necessarily built to interoperate with each other from a
logging/tracking perspective

this code cannot be very prescriptive about how a component records its
event information.
