# data manager framework from benc's perspective

## Intro

Staging in this abstraction is the act of taking a file "at rest"
and making its contents visible via posix file accesses to a parsl task
(or vice-versa).

Flush away your other preconceptions.

## requirements

this is a bit of a random grab bag from my head - needs to align with what
is in the shared google doc.

support per-task workdirs (for bash apps - for python apps there is no
per-task pwd to be able to change)

support mode where no filesystem shared between different workers in
the same executor

support complex movements - for example globus transfer from remote location
into a worker which has no shared fs (for example, by going via submit-side
or other location)

support staging from the submit side local file system

## staging methods - real and imagined

There are various staging methods. Some implemented, some not implemented. 
They do not all play nicely together.

- no parsl-explicit staging. Everything that needs to access a file needs
  to have posix access to files. Generally transfer of data to/from a worker
  node from/to its resting location happens by virtue of posix access on
  a shared filesystem: tasks use posix file opens, reads, writes and the
  underlying shared filesystem takes care of any necessary network activity.
  These

- staging where a separate parsl task runs on an executor site for each
  transfer of a file in/out.
  In the existing codebase, this only works when
  there is a shared filesystem between all workers so that transfers executed
  on one worker are visible to all other workers.
  This mode lets the parsl submit side have quite a lot of control about
  when things are staged (for example, a cache could be managed on the submit
  side) and also has potential for the submit side to use the existing
  task dag stuff to order multiple tasks which depend on a particular staging
  action happening

- staging where a separate parsl task runs on the submit side - for example,
  globus staging. This happens when the staging code does not need posix
  access to the executor file system - for example, with globus staging,
  that is because all executor-side filesystem access happens via globus
  endpoints. This staging is limited to staging between globus endpoints,
  so on the executor side, a globus endpoint must overlap with the relevant
  posix-accessible working directory.

- staging by the executor as part of job submission.

  For example,
  workqueue has a condor-style input and output file specification. This
  allows workqueue (and condor before it) to move files from the submit side
  file system into a workqueue (or condor) managed working directory.
  This style of staging probably can only stage files from the submit side
  posix file system into a task side working directory and back. It relies
  on the output file list being know at submission time (I think?) rather than
  being able to invent that list later. Although that isn't a problem with
  parsl's model at the moment, there is a feature request issue for this.

  Some executors use Channels, and for those executors, it is possible those
  executors could perform staging using the file transfer capabailities of
  Channels (for example, sftp staging to/from submit side filesystem)

- in-task staging - individual parsl tasks can call staging methods to
  transfer files in and out. those tasks will have access to their working
  directory - so a shared filesystem isn't necessary when using some network
  based staging such as http (c.f. separate task staging, where http staging
  would need to stage to a directory shared between all workers in an
  executor). However this makes managing of caching/task ordering harder:
  scoped files are implicitly scoped to a particular task rather than being
  easily shareable.

- composite staging: when a particular staging (for example, from globus
  into workqueue) is desired but can't be done as a single step, it might
  be possible to do as several steps: for example, a globus transfer to
  the submit side, and a workqueue job-submission staging from there.

- the DM2.0 google doc talks about various other staging mechanisms
  (eg inside zmq, or using rsync)

## other concepts

- Files "at rest" have a name/URI: at present, this can either be a
  globally resolvable URI (such as a globus or http URI?) or a file: URI which
  means "don't do any staging and assume this path is accessible from
  everywhere that is necessary". This should be extended to allow specification
  of files on the submit side posix filesystem (for example, by repurposing the
  file: URI scheme). This name is represented at present by the `File` class.

- Staging in this abstraction is the act of taking a file "at rest"
  and making its contents visible via posix file accesses to a parsl task
  (or vice-versa).

  What that act actually is is deliberately vague: in some cases it will be
  no action at all, because files might be already accessible via posix.
  In other cases it might consist of a composite of multiple staging
  method steps.

## slightly more concrete staging

- a staging action takes a URI/name and turns it into some other URI/name
  that has the same file content: for example, http stage-in takes an
  http URI and gives us a local filesystem name that contains the same
  content: it both downloaded the file, and gives some rewritten URI/name.

- in that case, names that are not URIs - that is, just posix filenames,
  have some form of scope to describe which filesystem scope they are
  valid in - for example, submit side, executor shared, worker node local.

- staging, then, is a sequence of staging actions which take an at-rest
  URI and return a filesystem path scoped to the worker node.
  The sequence of actions needed will vary depending both on the URI
  (for example, staging in from globus does different things to staging
  in from http - in the present implementation, as hard coded choices)
  and on the configuration of the executor (does it have a shared file
  system? does it have a globus endpoint?)

- some staging configurations will have access to things like: a submit side
  globus endpoint with a suitable submit side working directory.
  executor-side shared workdir. (that isn't a concept that exists for
  executors in general - for example, workqueue)

## configuration

I don't imagine the end user usually configuring explicit sequences of
staging actions themselves, unless they are doing something quite novel
- instead common use cases would be captured as pre-defined configurations,
representing the various use cases in the DM2.0 document. These choices
should not exist as a splattering of `if` statements through the codebase
but fit into the configuration system.

I expect that some users *will* need to do their own more complicated
configurations, if only because of the variety of different systems out there.
Forcing a limited set of hardcoded staging patterns will exclude those
users from parsl.

The configurations would look something like this, but in the form of
some python data/code:

  for executor e1, URI scheme `http`,  stage in by:
       i) run the http staging code as part of the task, to the task-local
          work directory
  for executor e1, URI scheme `globus`, stage in by:
       i) run the globus staging code locally to a submit side local directory
      ii) run site->executor staging to executor shared directory

The present configuration for GlobusScheme is a rudimentary form of this.

## code implementation notes

- yadu has at least wanted to have the file object have some ability to
  transfer itself arbitrarily on demand by the user. That does not fit in
  with the staging model here where in some circustances the staging
  must happen as part of a task (workqueue or worker-side staging) rather
  than arbitrarily.

- The DM2.0 google doc contains a lot of stuff with datafutures being
  indexed by execution site. That also does not make sense with this staging
  model.
  It is unclear to me what the end user use cases for this indexing are, so
  this document makes no attempt to address those explictly.

