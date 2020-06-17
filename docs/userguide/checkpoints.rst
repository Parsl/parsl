.. _label-checkpointing:

Checkpointing
-------------

Large-scale Parsl programs are likely to encounter errors due to node failures, 
application or environment errors, and myriad other issues. Parsl offers an
application-level checkpointing model to improve resilience, fault tolerance, and
efficiency.

.. note::
   Checkpointing is *only* possible for apps which have app caching enabled.
   If app caching is disabled in the config ``Config.app_cache``, checkpointing will
   **not** work.

Parsl follows an incremental checkpointing model, where each checkpoint file contains
all results that have been updated since the last checkpoint.

When a Parsl program loads a checkpoint file and is executed, it will use 
checkpointed results for any apps that have been previously executed. 
Like app caching, checkpoints
use the hash of the app and the invocation input parameters to identify previously computed
results. If multiple checkpoints exist for an app (with the same hash)
the most recent entry will be used.

Parsl provides four checkpointing modes:

1. ``task_exit``: a checkpoint is created each time an app completes or fails
   (after retries if enabled). This mode minimizes the risk of losing information
   from completed tasks.

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'task_exit'


2. ``periodic``: a checkpoint is created periodically using a user-specified
   checkpointing interval. Results will be saved to the checkpoint file for
	 all tasks that have completed during this period.

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'periodic'
   >>> config.checkpoint_period = "01:00:00"

3. ``dfk_exit``: checkpoints are created when Parsl is
   about to exit. This reduces the risk of losing results due to
   premature program termination from exceptions, terminate signals, etc. However
   it is still possible that information might be lost if the program is
   terminated abruptly (machine failure, SIGKILL, etc.)

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'dfk_exit'

4. Manual: in addition to these automated checkpointing modes, it is also possible to manually initiate a checkpoint
   by calling ``DataFlowKernel.checkpoint()`` in the Parsl program code.


   >>> import parsl
   >>> from parsl.configs.local_threads import config
   >>> dfk = parsl.load(config)
   >>> ....
   >>> dfk.checkpoint()

In all cases the checkpoint file is written out to the ``runinfo/RUN_ID/checkpoint/`` directory.

.. Note:: Checkpoint modes ``periodic``, ``dfk_exit``, and ``manual`` can interfere with garbage collection.
          In these modes task information will be retained after completion, until checkpointing events are triggered.


Creating a checkpoint
^^^^^^^^^^^^^^^^^^^^^

Automated checkpointing must be explicitly enabled in the Parsl configuration.
There is no need to modify a Parsl  program as checkpointing will occur transparently.
In the following example, checkpointing is enabled at task exit. The results of
each invocation of the ``slow_double`` app will be stored in the checkpoint file.

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

Alternatively, manual checkpointing can be used to explictly specify when the checkpoint
file should be saved. The following example shows how manual checkpointing can be used.
Here, the ``dfk.checkpoint()`` function will save the results of the prior invocations 
of the ``slow_double`` app.

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
    for i in range(0, N):
        d.append(slow_double(i))

    # Wait for the results
    [i.result() for i in d]

    cpt_dir = dfk.checkpoint()
    print(cpt_dir)  # Prints the checkpoint dir


Resuming from a checkpoint
^^^^^^^^^^^^^^^^^^^^^^^^^^

When resuming a program from a checkpoint Parsl allows the user to select
which checkpoint file(s) to use. 
Checkpoint files are stored in the ``runinfo/RUNID/checkpoint`` directory.

The example below shows how to resume using all available checkpoints. 
Here, the program re-executes the same calls to the ``slow_double`` app
as above and instead of waiting for results to be computed, the values
from the checkpoint file are are immediately returned.

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

    # wait for results
    print([d[i].result() for i in range(5)])
