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


Providers and Launchers
-----------------------
Some executors are based on blocks of workers (for example the
`parsl.executors.HighThroughputExecutor`: the submit side requires a
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

File staging
------------
Parsl can copy input files from an arbitrary URL into a task's working
environment, and copy output files from a task's working environment to
an arbitrary URL. A small set of data staging providers is installed by default,
for ``file://`` ``http://`` and ``ftp://`` URLs. More data staging providers can
be added in the workflow configuration, in the ``storage`` parameter of the
relevant `ParslExecutor`. Each provider should subclass the `Staging` class.


Default stdout/stderr name generation
-------------------------------------
Parsl can choose names for your bash apps stdout and stderr streams
automatically, with the parsl.AUTO_LOGNAME parameter. The choice of path is
made by a function which can be configured with the ``std_autopath``
parameter of Parsl `Config`. By default, ``DataFlowKernel.default_std_autopath``
will be used.


Memoization/checkpointing
-------------------------

When parsl memoizes/checkpoints an app parameter, it does so by computing a
hash of that parameter that should be the same if that parameter is the same
on subsequent invocations. This isn't straightforward to do for arbitrary
objects, so parsl implements a checkpointing hash function for a few common
types, and raises an exception on unknown types:

.. code-block::

  ValueError("unknown type for memoization ...")

You can plug in your own type-specific hash code for additional types that
you need and understand using `id_for_memo`.


Invoking other asynchronous components
--------------------------------------

Parsl code can invoke other asynchronous components which return Futures, and
integrate those Futures into the task graph: Parsl apps can be given any
`concurrent.futures.Future` as a dependency, even if those futures do not come
from invoking a Parsl app. This includes as the return value of a
``join_app``.

An specific example of this is integrating Globus Compute tasks into a Parsl
task graph. See :ref:`label-join-globus-compute`

Dependency resolution
---------------------

When Parsl examines the arguments to an app, it uses a `DependencyResolver`.
The default `DependencyResolver` will cause Parsl to wait for
``concurrent.futures.Future`` instances (including `AppFuture` and
`DataFuture`), and pass through other arguments without waiting.

This behaviour is pluggable: Parsl comes with another dependency resolver,
`DEEP_DEPENDENCY_RESOLVER` which knows about futures contained with structures
such as tuples, lists, sets and dicts.

This plugin interface might be used to interface other task-like or future-like
objects to the Parsl dependency mechanism, by describing how they can be
interpreted as a Future.

Removed interfaces
------------------

Parsl had a deprecated ``Channel`` abstraction. See
`issue 3515 <https://github.com/Parsl/parsl/issues/3515>`_
for further discussion on its removal.
