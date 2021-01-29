.. _label-joinapp:

Join Apps
=========

Join apps allows an app to define a sub-workflow: the app can launch other apps
and incorporate them into the main task graph. They can be specified using the
`join_app` decorator.

Join apps allow more naunced dependencies to be expressed that can help with:

* increased concurrency - helping with strong scaling
* more focused error propagation - allowing more of an ultimately failing workflow to complete
* more useful monitoring information

Usage
-----

A `join_app` looks quite like a python app, but should return a ``Future``,
rather than a value. After the python code has run, the app invocation will not
complete until that future has completed, and the return value of the `join_app`
will be the return value (or exception) from the returned future.

For example:

.. code-block:: python

  @python_app
  def some_app():
    return 3

  @join_app
  def example():
    x: Future = some_app()
    return x  # note that x is a Future, not a value

  # example.result() == 3

What/why/how can you do with a join app
---------------------------------------

join apps are useful when a workflow needs to launch some apps, but it doesn't
know what those apps are until some earlier apps are completed.

For example, a pre-processing stage might be followed by n middle stages,
but the value of n is not known until pre-processing is complete; or the
choice of app to run might depend on the output of pre-processing.

In the following example, a pre-processing stage is followed by a choice of
option 1 or option 2 apps, with a post-processing stage afterwards. All of the
example apps are toy apps that are intended to demonstrate control/data flow
but they are based on a real use case.

Here is the implementation using join apps. Afterwards, there are some
examples of the problems that arise trying to implement this without join apps.

.. code-block:: python

  @python_app
  def pre_process():
    return 3

  @python_app
  def option_one(x):
    # do some stuff
    return x*2

  @python_app
  def option_two(x):
    # do some more stuff
    return (-x) * 2

  @join_app
  def process(x):
    if x > 0:
      return option_one(x)
    else:
      return option_two(x)

  @python_app
  def post_process(x):
    return str(x) # convert x to a string

  # here is a simple workflow using these apps:
  # post_process(process(pre_process()))).result() == "6"
  # pre_process gives the number 3, process turns it into 6,
  # and post_process stringifys it to "6" 

So why do we need process to be a ``@join_app`` for this to work?

* Why can't process be a regular python function?

``process`` needs to inspect the value of ``x`` to make a decision about
what app to launch. So it needs to defer execution until after the
pre-processing stage has completed. In parsl, the way to defer that is
using apps: the execution of process will happen when the future returned
by pre_process has completed.

* Why can't process be a @python_app?

A python app, if run in a `ThreadPoolExecutor`, can launch more parsl apps;
so a python app implementation of process() would be able to inspect x and
launch ``option_{one, two}``.

From launching the ``option_{one, two}`` app, the app body python code would
get a ``Future[int]`` - a ``Future`` that will eventually contain ``int``.

But now, we want to (at submission time) invoke post_process, and have it wait
until the relevant ``option_{one, two}`` app has completed.

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

Terminology
-----------

The term "join" comes from use of monads in functional programming, especially Haskell.
