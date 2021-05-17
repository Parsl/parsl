Plugins
=======

Parsl has several places where code can be plugged in. Parsl usually provides
several implementations that use each plugin point.

This page gives a brief summary of those places and why you might want
to use them, with links to the API guide.

Executors
---------
When the parsl dataflow kernel is ready for a task to run, it passes that
task to an `ParslExecutor`. The executor is then responsible for running the task's
Python code and returning the result. This is the abstraction that allows one
executor to run code on the local submitting host, while another executor can
run the same code on a large supercomputer.


Providers, Launchers and Channels
---------------------------------
Some executors are based on blocks of workers (for example the
`HighThroughputExecutor`: the submit side requires a
batch system (eg slurm, kubernetes) to start worker processes, which then
execute tasks.

The particular way in which a system makes those workers start is implemented
by providers and launchers.

An `ExecutionProvider` allows a command line to be submitted as a request to the
underlying batch system to be run inside an allocation of nodes.

A `Launcher` modifies that command line when run inside the allocation to
add on any wrappers that are needed to launch the command (eg srun inside
slurm). Providers and launchers are usually paired together for a particular
system type.

A `Channel` allows the commands used to interact with an `ExecutionProvider` to be
executed on a remote system. The default channel executes commands on the
local system, but a few variants of an `SSHChannel` are provided.


File staging
------------
Parsl can copy input files from an arbitrary URL into a task's working
environment, and copy output files from a task's working environment to
an arbitrary URL. A small set of data staging providers is installed by default,
for ``file://`` ``http://`` and ``ftp://`` URLs. More data staging providers can
be added in the workflow configuration, in the ``storage`` parameter of the
relevant `ParslExecutor`. Each provider should subclass the `Staging` class.


Memoization/checkpointing
-------------------------

When parsl memoizes/checkpoints an app parameter, it does so by computing a
hash of that parameter that should be the same if that parameter is the same
on subsequent invocations. This isn't straightforward to do for arbitrary
objects, so parsl implements a checkpointing hash function for a few common
types, and raises an exception on unknown types (TK put in unknown exception
example text here so searching finds it).

You can plug in your own type-specific hash code for additional types that
you need and understand using `id_for_memo`.
