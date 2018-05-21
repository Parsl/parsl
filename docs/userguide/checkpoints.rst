.. _label-checkpointing:

Checkpointing
-------------

Large scale workflows are prone to errors due to node failures, application or environment errors, and myriad other issues. Parsl's support for checkpointing provides workflow resilence and fault tolerance. 

.. note::
   Checkpointing is *only* possible for apps which have AppCaching enabled.
   If AppCaching is disabled in ``config['globals']``, checkpointing will
   **not** work.

Parsl follows an incremental checkpointing model, where each checkpoint contains
all results that have been updated since the last checkpoint. 

When loading a checkpoint the Parsl script will use checkpointed results for 
any apps that have been previously executed. Like AppCaching, checkpoints
use the app name, hash, and input parameters to locate previously computed
results. If multiple checkpoints exist for an app (with the same hash)
the most recent entry will be used.

Checkpointing works in the following modes:

1. ``task_exit``: In this mode, a checkpoint is created each time an app completes or fails
   (after retries if enabled). This mode reduces the risk of losing information
   from completed tasks to a minimum.

   >>> config["globals"]["checkpointMode"] = 'task_exit'


2. ``periodic``: The periodic mode allows the user to specify the interval at which
   all tasks are checkpointed.

   >>> config["globals"]["checkpointMode"] = 'periodic'
   >>> config["globals"]["checkpointPeriod"] = "01:00:00"

3. ``dfk_exit``: In this mode, checkpoints are created when the DataFlowKernel is
   about to exit. This reduces the risk of losing results due to
   premature workflow termination from exceptions, terminate signals, etc. However
   there's still some likelihood that information might be lost if the workflow is
   terminated abruptly (machine failure, SIGKILL etc)

   >>> config["globals"]["checkpointMode"] = 'dfk_exit'

4. Manual: In addition to these automated checkpointing modes, it is also possible to manually initiate a checkpoint
   by calling ``DataFlowKernel.checkpoint()`` in the workflow code.

In all cases the checkpoint is written out to the ``runinfo/RUN_ID/checkpoint/`` directory.

Creating a checkpoint
^^^^^^^^^^^^^^^^^^^^^

When using automated checkpointing there is no need for users to modify their
Parsl script in any way: checkpointing will be conducted completely automatically. 
The following example shows how manual checkpointing can be invoked in a Parsl script. 

.. code-block:: python

    from parsl import *
    from parsl.tests.configs.local_threads import config

    dfk = DataFlowKernel(config=config)

    @App('python', dfk, cache=True)
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


Loading a checkpoint
^^^^^^^^^^^^^^^^^^^^

To load a checkpoint the user must select which checkpoint file to resume from. 
As mentioned above, checkpoint files are stored in the ``runinfo/RUNID/checkpoint`` directory.
The example below shows how to resume using from all available checkpoints:

.. code-block:: python

    from parsl import *
    from parsl.tests.configs.local_threads import config
    from parsl.utils import get_all_checkpoints

    dfk = DataFlowKernel(config=config,
                         checkpointFiles=parsl.get_all_checkpoints())
