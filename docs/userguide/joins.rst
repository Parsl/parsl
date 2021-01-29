Join Apps
=========

Join apps allows an app to define a sub-workflow: the app can launch other apps
and incorporate them into the main task graph. They can be specified using the
`join_app` decorator.

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

A python app, if run in a ThreadPoolExecutor, can launch more parsl apps;
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

Now that for loop can only iterate after pre_processing is done for each
iteration - it is unnecessarily serialised so that pre_processing cannot run
in parallel.

So, the rule about not calling ``.result()`` applies in the main workflow thread
too.

What join apps add is the ability for parsl to unwrap that Future[Future[int]] into a
Future[int] in a "sensible" way (eg it doesn't need to block a worker).

Motivation
----------

Although apps can be launched from within a `python_app` in any `ThreadPoolExecutor`,
the ``Future`` objects from those launched apps cannot easily be used.

Here is a motivating example that shows ways in which launching apps from inside a
`python_app` is insufficient.

Consider a workflow where there are "sensors" which must be processed and assembled
into "patches", and then all patches assembled into a "mosaic".

The list of which sensors must be assembled into which "patches" is not known ahead
of time and is expensive to compute.

There are many patches that must all be processed in this way.

For each patch:

1. Generate list of sensors for this patch, in a single parsl app.
2. Process sensors for this patch - one parsl app per sensor
3. When all sensors are processed, assemble all the processed data into a patch.
4. When assembly is complete, do some post processing on the patch

Once all patches are done:

5. Make mosaic of all patches

Here are some possible implementations and their weaknesses.

Attempt 1 - a simple for loop
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  post_process_futures = []
  for patch in patch_list:
    sensor_list_future = generate_sensor_list_app(patch)
    sensor_futures = []
    for sensor in sensor_list_future.result():
      sensor_futures.append(process_sensor(sensor))
    assembled_future = assemble_sensors(*sensor_futures)
    post_process_futures.append(post_process(assembled_future))
  mosaic_future = make_mosaic(*post_process_futures)
  mosaic_future.result()

Weaknesses
""""""""""

*  Only one patch is processed at a time. The outer for loop blocks on task completion repeatedly
   before all of the apps are submitted, forcing unnecessarily serialised execution ordering: each
   loop will block waiting for generate_sensor_list_app to complete.

Attempt 2 - a `python_app` per iteration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Put the whole loop body into an app.

.. code-block:: python

  patch_futures = []
  for patch in patch_list:
    patch_futures = process_patch(patch)
  mosaic_future = make_mosaic(*patch_futures)
  mosaic_future.result()

  @python_app(executors=['local-thread-pool']
  def process_patch():
    sensor_list_future = generate_sensor_list_app(patch)
    sensor_futures = []
    for sensor in sensor_list_future.result():
      sensor_futures.append(process_sensor(sensor))
    assembled_future = assemble_sensors(*sensor_futures)
    post_process_future = post_process(assembled_future)
    post_process_future.result()

In this attempt, all of the for-loop bodies are launched without blocking (as process_patch invocations),
and potentially can run concurrently.

Weaknesses
""""""""""

Each process_patch app must occupying a thread pool worker for the entire duration of
the tasks that it has launched, because it blocks waiting for completion of the post_process step, by
calling post_process_future.result().

It does this so that the process_patch app completes after the post_process step. If this call to
.result() was not there, the process_patch app invocation would complete too early, and make_mosaic
could run before post_process is complete.

Because of this, there can be serialization and deadlock issues: there needs to be one local-thread-pool
worker available for every patch to be processed simultanously, that will for the most part be sitting
idle waiting for final results. If there are fewer workers, then patch processing will be
serialised due to lack of workers, although to a lesser extent than in attempt 1.

But worse, if any of the launched apps also used the same `ThreadPoolExecutor`, then
the workflow can deadlock:
launched process_patch apps will be waiting for other apps to complete, but those apps cannot start
because process_patch apps are occupying all of the `ThreadPoolExecutor` workers.

This leads to a principle: apps should not block on other apps; instead any blocking of execution
should happen inside parsl's dependency mechanism.

Attempt 3 - No blocking inside apps
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


.. code-block:: python

  sensor_list_futures = []
  for patch in patch_list:
    future = generate_sensor_list_app(patch)
    future.patch = patch
    sensor_list_futures.append(future)

  post_process_futures=[]
  for future in concurrent.futures.as_completed(sensor_list_futures):
    patch = future.patch
    sensor_futures = []
    for sensor in future.result()
      sensor_futures.append(process_sensor(sensor)
    assembled_future = assemble_sensors(*sensor_futures)
    post_process_futures.append(post_process(assembled_future))

  mosaic_future = make_mosaic(*post_process_futures)
  mosaic_future.result()

Weaknesses
""""""""""

There is some loss of the sense of data flow expressed in syntax, obscured by
the implementation of a rudimentary in-workflow task scheduler that only knows about sensor list futures.

This is *still* blocking on the second for loop needing to complete before later apps can be launched,
and that for loop only completes when all sensor lists have been generated - this is earlier than
in previous examples, but there is still blocking there.

This impedes compositionality: this code could not be placed into a function and (for example) run
inside another for loop that loops over multiple datasets: the function would block each time waiting
for sensor list generation, rather than processing each dataset's sensor list generations
concurrently.

Anything that blocks the execution thread on future completion (for example, ``.result()``
or ``.as_completed()``) is the enemy.

`join_app` syntax
------------------

This is an attempt to move some of the cases where blocking and ad-hoc task scheduling happens in
the workflow into parsl dependency handling. The only blocking should happen at the very end of the
workflow, so that the main process does not end until work is completed. Other than that, nothing else
in the user workflow should block waiting for app completion.

.. code-block:: python

  post_process_futures=[]
  for patch in patch_list:
    sensor_list_future = generate_sensor_list_app(patch)
    sensors_future = process_sensors(sensor_list_future)
    assembled_future = assemble_sensors(sensors_future)
    post_process_futures.append(post_process(assembled_future))

  mosaic_future = make_mosaic(*post_process_futures)
  mosaic_future.result()

  @join_app
  def process_sensors(sensor_list):
    sensor_futures = []
    for sensor in sensor_list:
      sensor_futures.append(process_sensor(sensor))
    return combine(*sensor_futures)

  @python_app
  def combine(*args):
    pass # do nothing, but only after all args are complete



This example uses a helper app called ``combine`` which, given a list of input futures,
completes when all of those futures complete, without any further processing. This constructs a
barrier future, depending on an arbitrary list of other futures.

This allows more naunced dependencies to be expressed that can help with:

* increased concurrency - helping with strong scaling
* more focused error propagation - allowing more of an ultimately failing workflow to complete
* more useful monitoring information

Terminology
-----------

The term "join" comes from use of monads in functional programming, especially Haskell.
