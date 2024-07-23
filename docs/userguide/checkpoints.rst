.. _label-memos:

Memoization and Checkpointing
-----------------------------

When an app is invoked several times with the same parameters, Parsl can reuse the result from the first invocation without executing the app again. This can save time and computational resources.

This is done in two ways:

- **App caching**: Allows reuse of results within the same run.
- **Checkpointing**: Stores results on the filesystem and reuses those results in later runs.

.. _label-appcaching:

App Caching
===========

There are many situations in which a program may be re-executed over time. Often, large fragments of the program will not have changed, and re-execution of apps will waste valuable time and computation resources. Parsl's app caching solves this problem by storing results from apps that have successfully completed so that they can be re-used.

App caching is enabled by setting the ``cache`` argument in the `parsl.app.app.python_app` or `parsl.app.app.bash_app` decorator to ``True`` (by default it is ``False``).

.. code-block:: python

   @bash_app(cache=True)
   def hello(msg, stdout=None):
       return 'echo {}'.format(msg)

App caching can be globally disabled by setting ``app_cache=False`` in the `parsl.config.Config`.

App caching can be particularly useful when developing interactive programs, such as when using a Jupyter notebook. In this case, cells containing apps are often re-executed during development. Using app caching will ensure that only modified apps are re-executed.

App Equivalence
^^^^^^^^^^^^^^^

Parsl determines app equivalence by storing the hash of the app function. Thus, any changes to the app code (e.g., its signature, its body, or even the docstring within the body) will invalidate cached values.

However, Parsl does not traverse the call graph of the app function, so changes inside functions called by an app will not invalidate cached values.

Invocation Equivalence
^^^^^^^^^^^^^^^^^^^^^^

Two app invocations are determined to be equivalent if their input arguments are identical.

In simple cases, this follows obvious rules:

.. code-block:: python

  # These two app invocations are the same and the second invocation will
  # reuse the cached result from the first invocation
  x = 7
  f(x).result()

  y = 7
  f(y).result()

Internally, equivalence is determined by hashing the input arguments and comparing the hash to hashes from previous app executions.

This approach can only be applied to data types for which a deterministic hash can be computed.

By default, Parsl can compute sensible hashes for basic data types: str, int, float, None, as well as some complex types: functions, dictionaries, and lists containing hashable types.

Attempting to cache apps invoked with other, non-hashable data types will lead to an exception at invocation.

In that case, mechanisms to hash new types can be registered by implementing the `parsl.dataflow.memoization.id_for_memo` function for the new type.

Ignoring Arguments
^^^^^^^^^^^^^^^^^^

On occasion, you may wish to ignore particular arguments when determining app invocation equivalence, for example, when generating log file names automatically based on time or run information. Parsl allows developers to list the arguments to be ignored in the ``ignore_for_cache`` app decorator parameter:

.. code-block:: python

   @bash_app(cache=True, ignore_for_cache=['stdout'])
   def hello(msg, stdout=None):
       return 'echo {}'.format(msg)

Caveats
^^^^^^^

There are important considerations when using app caching:

- **Determinism**: App caching is generally useful only when the apps are deterministic. If the outputs may be different for identical inputs, app caching will obscure this non-deterministic behavior. For instance, caching an app that returns a random number will result in every invocation returning the same result.
- **Timing**: If several identical calls to an app are made concurrently before a result is cached, many instances of the app will be launched. Once one invocation completes and the result is cached, all subsequent calls will return immediately with the cached result.
- **Performance**: If app caching is enabled, there may be some performance overhead, especially if a large number of short-duration tasks are launched rapidly. This overhead has not been quantified.

.. _label-checkpointing:

Checkpointing
=============

Large-scale Parsl programs are likely to encounter errors due to node failures, application or environment errors, and myriad other issues. Parsl offers an application-level checkpointing model to improve resilience, fault tolerance, and efficiency.

.. note::
   Checkpointing builds on top of app caching, so app caching must be enabled. If app caching is disabled in the config (`Config.app_cache`), checkpointing will not work.

Parsl follows an incremental checkpointing model, where each checkpoint file contains all results that have been updated since the last checkpoint.

When a Parsl program loads a checkpoint file and is executed, it will use checkpointed results for any apps that have been previously executed. Like app caching, checkpoints use the hash of the app and the invocation input parameters to identify previously computed results. If multiple checkpoints exist for an app (with the same hash), the most recent entry will be used.

Parsl provides four checkpointing modes:

1. **task_exit**: A checkpoint is created each time an app completes or fails (after retries if enabled). This mode minimizes the risk of losing information from completed tasks.

   .. code-block:: python

      from parsl.configs.local_threads import config
      config.checkpoint_mode = 'task_exit'

2. **periodic**: A checkpoint is created periodically using a user-specified checkpointing interval. Results will be saved to the checkpoint file for all tasks that have completed during this period.

   .. code-block:: python

      from parsl.configs.local_threads import config
      config.checkpoint_mode = 'periodic'
      config.checkpoint_period = "01:00:00"

3. **dfk_exit**: Checkpoints are created when Parsl is about to exit. This reduces the risk of losing results due to premature program termination from exceptions, terminate signals, etc. However, information might still be lost if the program is terminated abruptly (e.g., machine failure, SIGKILL).

   .. code-block:: python

      from parsl.configs.local_threads import config
      config.checkpoint_mode = 'dfk_exit'

4. **manual**: In addition to automated checkpointing modes, you can manually initiate a checkpoint by calling ``DataFlowKernel.checkpoint()`` in the Parsl program code.

   .. code-block:: python

      import parsl
      from parsl.configs.local_threads import config
      dfk = parsl.load(config)
      ....
      dfk.checkpoint()

In all cases, the checkpoint file is written out to the ``runinfo/RUN_ID/checkpoint/`` directory.

.. note::
   Checkpoint modes ``periodic``, ``dfk_exit``, and ``manual`` can interfere with garbage collection. In these modes, task information is retained after completion until checkpointing events are triggered.

Creating a Checkpoint
^^^^^^^^^^^^^^^^^^^^^

Automated checkpointing must be explicitly enabled in the Parsl configuration. There is no need to modify a Parsl program, as checkpointing occurs transparently. In the following example, checkpointing is enabled at task exit. The results of each invocation of the ``slow_double`` app will be stored in the checkpoint file.

.. code-block:: python

    import parsl
    from parsl.app.app import python_app
    from parsl.configs.local_threads import config

    config.checkpoint_mode = 'task_exit'

    parsl.load(config)

    @python_app(cache=True)
    def slow_double(x):
        import time
        time.sleep(5)
        return x * 2

    d = []
    for i in range(5):
        d.append(slow_double(i))

    print([d[i].result() for i in range(5)])

Alternatively, you can use manual checkpointing to specify when the checkpoint file should be saved. The following example shows how manual checkpointing can be used. Here, the ``dfk.checkpoint()`` function saves the results of the prior invocations of the ``slow_double`` app.

.. code-block:: python

    import parsl
    from parsl import python_app
    from parsl.configs.local_threads import config

    dfk = parsl.load(config)

    @python_app(cache=True)
    def slow_double(x, sleep_dur=1):
        import time
        time.sleep(sleep_dur)
        return x * 2

    N = 5   # Number of calls to slow_double
    d = []  # List to store the futures
    for i in range(N):
        d.append(slow_double(i))

    # Wait for the results
    [i.result() for i in d]

    cpt_dir = dfk.checkpoint()
    print(cpt_dir)  # Prints the checkpoint directory

Resuming from a Checkpoint
^^^^^^^^^^^^^^^^^^^^^^^^^^

When resuming a program from a checkpoint, Parsl allows you to select which checkpoint files to use. Checkpoint files are stored in the ``runinfo/RUNID/checkpoint`` directory.

The example below shows how to resume using all available checkpoints. Here, the program re-executes the same calls to the ``slow_double`` app as above. Instead of waiting for results to be computed, the values from the checkpoint file are returned immediately.

.. code-block:: python

    import parsl
    from parsl.tests.configs.local_threads import config
    from parsl.utils import get_all_checkpoints

    config.checkpoint_files = get_all_checkpoints()

    parsl.load(config)

    # Rerun the same workflow
    d = []
    for i in range(5):
        d.append(slow_double(i))

    # Wait for results
    print([d[i].result() for i in range(5)])
