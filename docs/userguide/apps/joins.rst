.. _label-joinapp:

Join Apps
=========

Join apps, defined with the ``@join_app`` decorator, are a form of app that can
launch other pieces of a workflow: for example a Parsl sub-workflow, or a task
that runs in some other system.

Parsl sub-workflows
-------------------

One reason for launching Parsl apps from inside a join app, rather than
directly in the main workflow code, is because the definitions of those tasks
are not known well enough at the start of the workflow.

For example, a workflow might run an expensive step to detect some objects
in an image, and then on each object, run a further expensive step. Because
the number of objects is not known at the start of the workflow, but instead
only after an expensive step has completed, the subsequent tasks cannot be
defined until after that step has completed.

In simple cases, the main workflow script can be stopped using
``Future.result()`` and join apps are not necessary, but in more complicated
cases, that approach can severely limit concurrency.

Join apps allow more naunced dependencies to be expressed that can help with:

* increased concurrency - helping with strong scaling
* more focused error propagation - allowing more of an ultimately failing workflow to complete
* more useful monitoring information

Using Futures from other components
-----------------------------------

Sometimes, a workflow might need to incorporate tasks from other systems that
run asynchronously but do not need a Parsl worker allocated for their entire
run. An example of this is delegating some work into Globus Compute: work can
be given to Globus Compute, but Parsl does not need to keep a worker allocated
to that task while it runs. Instead, Parsl can be told to wait for the ``Future``
returned by Globus Compute to complete.

Usage
-----

A `join_app` looks quite like a `python_app`, but should return a ``Future``,
or a list of ``Future`` objects, rather than a value. Once the Python code has
run, the app will wait for those Futures to complete without occupying a Parsl
worker, and when those Futures complete, their contents will be the return
value of the `join_app`.

For example:

.. code-block:: python

  @python_app
  def some_app():
    return 3

  @join_app
  def example():
    x: Future = some_app()
    return x  # note that x is a Future, not a value

  assert example.result() == 3

Example of a Parsl sub-workflow
-------------------------------

This example workflow shows a preprocessing step, followed by
a middle stage that is chosen by the result of the pre-processing step
(either option 1 or option 2) followed by a know post-processing step.

.. code-block:: python

  @python_app
  def pre_process():
    return 3

  @python_app
  def option_one(x):
    return x*2

  @python_app
  def option_two(x):
    return (-x) * 2

  @join_app
  def process(x):
    if x > 0:
      return option_one(x)
    else:
      return option_two(x)

  @python_app
  def post_process(x):
    return str(x)

  assert post_process(process(pre_process()))).result() == "6"

* Why can't process be a regular python function?

``process`` needs to inspect the value of ``x`` to make a decision about
what app to launch. So it needs to defer execution until after the
pre-processing stage has completed. In Parsl, the way to defer that is
using apps: even though ``process`` is invoked at the start of the workflow,
it will execute later on, when the Future returned by ``pre_process`` has a
value.

* Why can't process be a @python_app?

A Python app, if run in a `parsl.executors.ThreadPoolExecutor`, can launch
more parsl apps; so a ``python_app`` implementation of process() would be able
to inspect x and choose and invoke the appropriate ``option_{one, two}``.

From launching the ``option_{one, two}`` app, the app body python code would
get a ``Future[int]`` - a ``Future`` that will eventually contain ``int``.

But, we want to invoke ``post_process`` at submission time near the start of
workflow so that Parsl knows about as many tasks as possible. But we don't
want it to execute until the value of the chosen ``option_{one, two}`` app
is known.

If we don't have join apps, how can we do this?

We could make process wait for ``option_{one, two}`` to complete, before
returning, like this:

.. code-block:: python

  @python_app
  def process(x):
    if x > 0:
      f = option_one(x)
    else:
      f = option_two(x)
    return f.result()

but this will block the worker running ``process`` until ``option_{one, two}``
has completed. If there aren't enough workers to run ``option_{one, two}`` this
can even deadlock. (principle: apps should not wait on completion of other 
apps and should always allow parsl to handle this through dependencies)

We could make process return the ``Future`` to the main workflow thread:

.. code-block:: python

  @python_app
  def process(x):
    if x > 0:
      f = option_one(x)
    else:
      f = option_two(x)
    return f  # f is a Future[int]

  # process(3) is a Future[Future[int]]


What comes out of invoking ``process(x)`` now is a nested ``Future[Future[int]]``
- it's a promise that eventually process will give you a promise (from
``option_one, two}``) that will eventually give you an int.

We can't pass that future into post_process... because post_process wants the
final int, and that future will complete before the int is ready, and that
(outer) future will have as its value the inner future (which won't be complete yet).

So we could wait for the result in the main workflow thread:

.. code-block:: python

  f_outer = process(pre_process())  # Future[Future[int]]
  f_inner = f_outer.result  # Future[int]
  result = post_process(f_inner)
  # result == "6"

But this now blocks the main workflow thread. If we really only need to run
these three lines, that's fine, but what about if we are in a for loop that
sets up 1000 parametrised iterations:

.. code-block:: python

  for x in [1..1000]:
    f_outer = process(pre_process(x))  # Future[Future[int]]
    f_inner = f_outer.result()  # Future[int]
    result = post_process(f_inner)

The ``for`` loop can only iterate after pre_processing is done for each
iteration - it is unnecessarily serialised by the ``.result()`` call, 
so that pre_processing cannot run in parallel.

So, the rule about not calling ``.result()`` applies in the main workflow thread
too.

What join apps add is the ability for parsl to unwrap that Future[Future[int]] into a
Future[int] in a "sensible" way (eg it doesn't need to block a worker).


.. _label-join-globus-compute:

Example of invoking a Futures-driven task from another system
-------------------------------------------------------------


This example shows launching some activity in another system, without
occupying a Parsl worker while that activity happens: in this example, work is
delegated to Globus Compute, which performs the work elsewhere. When the work
is completed, Globus Compute will put the result into the future that it
returns, and then (because the Parsl app is a ``@join_app``), that result will
be used as the result of the Parsl app.

As above, the motivation for doing this inside an app, rather than in the
top level is that sufficient information to launch the Globus Compute task
might not be available at start of the workflow.

This workflow will run a first stage, ``const_five``, on a Parsl worker,
then using the result of that stage, pass the result as a parameter to a
Globus Compute task, getting a ``Future`` from that submission. Then, the
results of the Globus Compute task will be passed onto a second Parsl
local task, ``times_two``.

.. code-block:: python

  import parsl
  from globus_compute_sdk import Executor

  tutorial_endpoint_uuid = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
  gce = Executor(endpoint_id=tutorial_endpoint_uuid)

  def increment_in_funcx(n):
      return n+1

  @parsl.join_app
  def increment_in_parsl(n):
      future = gce.submit(increment_in_funcx, n)
      return future

  @parsl.python_app
  def times_two(n):
      return n*2

  @parsl.python_app
  def const_five():
      return 5

  parsl.load()

  workflow = times_two(increment_in_parsl(const_five()))

  r = workflow.result()

  assert r == (5+1)*2


Terminology
-----------

The term "join" comes from use of monads in functional programming, especially Haskell.
