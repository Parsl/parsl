.. _label-parsl-perf:

Measuring performance with parsl-perf
=====================================

``parsl-perf`` is tool for making basic performance measurements of Parsl
configurations.

It runs repeated batches of no-op apps, giving a measurement of tasks per
second. By default, ``parsl-perf`` will attempt to estimate a batch size
that will take 2 minutes to execute.

This can give a basic measurement of some of the overheads in task
execution.

``parsl-perf`` must be invoked with a configuration file, which is a Python
file containing a variable ``config`` which contains a `Config` object, or
a function ``fresh_config`` which returns a `Config` object. The
``fresh_config`` format is the same as used with the pytest test suite.

To specify a ``parsl_resource_specification`` for tasks, add a ``--resources``
argument.

To change the target runtime from the default of 120 seconds, add a
``--time`` parameter.

To change the iteration algorithm used to pick the next batch size and to
decide when to stop, use the ``--iterate`` parameter:

 * ``--iterate=estimate`` (the default) will run a small batch, then
   repeatedly re-estimate a new batch size until it finds one that takes at least
   75% of the target run time. This will rapidly find a batch size of around
   the target size.

 * ``--iterate=exponential`` will start with a small batch, then double the size
   in each subsequent batch until reaching the target run time. This is useful
   for understanding how a configuration scales down as batch sizes increase,
   which is sometimes sub-linear, and to get more consistent batch sizes across
   runs of ``parsl-perf``.

For example:

.. code-block:: bash


    $ python -m parsl.benchmark.perf --config parsl/tests/configs/workqueue_ex.py --resources '{"cores":1, "memory":0, "disk":0}'
    ==== Iteration 1 ====
    Will run 10 tasks to target 120 seconds runtime
    Submitting tasks / invoking apps
    warning: using plain-text when communicating with workers.
    warning: use encryption with a key and cert when creating the manager.
    All 10 tasks submitted ... waiting for completion
    Submission took 0.008 seconds = 1248.676 tasks/second
    Runtime: actual 3.668s vs target 120s
    Tasks per second: 2.726

    [...]

    ==== Iteration 4 ====
    Will run 57640 tasks to target 120 seconds runtime
    Submitting tasks / invoking apps
    All 57640 tasks submitted ... waiting for completion
    Submission took 34.839 seconds = 1654.487 tasks/second
    Runtime: actual 364.387s vs target 120s
    Tasks per second: 158.184
    Cleaning up DFK
    The end
    
