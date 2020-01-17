.. _label-checkpointing:

Checkpointing
-------------

Large scale workflows are prone to errors due to node failures, application or environment errors, and myriad other issues. 
Parsl's checkpointing model provides workflow resilience and fault tolerance.

.. note::
   Checkpointing is *only* possible for apps which have app caching enabled.
   If app caching is disabled in the config ``Config.app_cache``, checkpointing will
   **not** work.

Parsl follows an incremental checkpointing model, where each checkpoint file contains
all results that have been updated since the last checkpoint.

When loading a checkpoint file the Parsl script will use checkpointed results for
any apps that have been previously executed. Like app caching, checkpoints
use the app name, hash, and input parameters to locate previously computed
results. If multiple checkpoints exist for an app (with the same hash)
the most recent entry will be used.

Parsl provides four checkpointing modes:

1. ``task_exit``: a checkpoint is created each time an app completes or fails
   (after retries if enabled). This mode minimizes the risk of losing information
   from completed tasks.

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'task_exit'


2. ``periodic``: a checkpoint is created periodically using a user-specified
   checkpointing interval.

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'periodic'
   >>> config.checkpoint_period = "01:00:00"

3. ``dfk_exit``: checkpoints are created when Parsl is
   about to exit. This reduces the risk of losing results due to
   premature workflow termination from exceptions, terminate signals, etc. However
   it is still possible that information might be lost if the workflow is
   terminated abruptly (machine failure, SIGKILL, etc.)

   >>> from parsl.configs.local_threads import config
   >>> config.checkpoint_mode = 'dfk_exit'

4. Manual: in addition to these automated checkpointing modes, it is also possible to manually initiate a checkpoint
   by calling ``DataFlowKernel.checkpoint()`` in the workflow code.


   >>> import parsl
   >>> from parsl.configs.local_threads import config
   >>> dfk = parsl.load(config)
   >>> ....
   >>> dfk.checkpoint()

In all cases the checkpoint file is written out to the ``runinfo/RUN_ID/checkpoint/`` directory.

.. Note:: Checkpoint modes `periodic`, `dfk_exit`, and `manual` can interfere with garbage collection.
          In these modes task information will be retained after completion, until checkpointing events are triggered.


Creating a checkpoint
^^^^^^^^^^^^^^^^^^^^^

When using automated checkpointing there is no need to modify a Parsl 
script as checkpointing will be conducted transparently.
The following example shows how manual checkpointing can be invoked in a Parsl script.

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
^^^^^^^^^^^^^^^^^^^^

When resuming a workflow from a checkpoint Parsl allows the user to select
which checkpoint file(s) to be used. 
As mentioned above, checkpoint files are stored in the ``runinfo/RUNID/checkpoint`` directory.
The example below shows how to resume using from all available checkpoints:

.. code-block:: python

    import parsl
    from parsl.tests.configs.local_threads import config
    from parsl.utils import get_all_checkpoints

    config.checkpoint_files = get_all_checkpoints()

    parsl.load(config)
