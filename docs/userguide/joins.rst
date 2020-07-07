Join Apps
=========

Join apps allow a workflow to asynchronously launch other apps and incorporate
them into the task graph. They can be specified using the `@join_app` decorator.

Although apps can be launched from within a `@python_app` in any `ThreadPoolExecutor`,
the `Future` objects for those launched apps cannot easily be used outside of the
launching `@python_app`.

Motivating example
------------------

Here is a motivating example which shows ways in which launching apps from inside a
`@python_app` is insufficient.

Consider a workflow where there are "sensors" which must be processed and assembled
into "patches", and then all patches assembled into a "mosaic".

The list of which sensors must be assembled into which "patches" is not know ahead
of time and is expensive to compute.

There are many patches which must all be processed in this way.

For each patch:
1. Generate list of sensors for this patch, in a single parsl app.
2. Process sensors for this patch - one parsl app per sensor
3. When all sensors are processed, assemble all the processed data into a patch.
4. When assembly is complete, do some post processing on the patch
Once all patches are done:
5. Make mosaic of all patches

Here are some implementation attempts and the weaknesses I see in them.

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

But worse, if any of the launched apps also used the `local-thread-pool` executor, then
the workflow can deadlock:
launched process_patch apps will be waiting for other apps to complete, but those apps cannot start
because process_patch apps are occupying all of the `local-thread-pool` workers.

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
for sensor list generation, rather than processing each data set's sensor list generations
concurrently.

Anything that blocks the execution thread on future completion (for example, `.result()`
or `.as_completed()`) is the enemy.


`@join_app` syntax
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

A join_app looks quite like a python app, but should return a future, rather than a value.
After the python code has run, the app invocation will not complete until that future has
completed, and the return value of the `@join_app` will be the return value (or exception)
from the returned future.

This example uses a helper app called `combine` which, given a list of input futures,
completes when all of those futures complete, without any further processing. This constructs a
barrier future, depending on an arbitrary list of other futures.

This allows more naunced dependencies to be expressed which can help with:
* increased concurrency - helping with strong scaling
* more focused error propagation - allowing more of an ultimately failing workflow to complete
* more useful monitoring information

Terminology
-----------

The term `join` comes from use of monads in functional programming, especially Haskell.

