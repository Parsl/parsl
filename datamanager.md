# data manager from benc's perspective


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

