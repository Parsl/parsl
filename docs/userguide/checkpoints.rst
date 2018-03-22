.. _label-checkpointing:

Checkpointing
-------------

Checkpointing extends AppCaching a step further and allows you to re-use
results from and across multiple workflows.

.. note::
   Checkpointing is *only* possible for apps which have AppCaching enabled.

.. note::
   If AppCaching is disabled in the ``config['globals']``, checkpointing will
   **not** work

Parsl follows an incremental checkpointing model, where each call to checkpoint
will checkpoint all results that have updated since the last checkpoint. When loading
checkpoints, if entries with results from multiple functions (with identical hashes)
are encountered, only the last entry read will be considered.

Checkpointing works in the following modes:

1. ``task_exit``: In this mode, each app is checkpointed when it has completed or
   failed (after retries if enabled). This mode reduces the risk of losing information
   from completed tasks to a minimum.

   >>> config["globals"]["checkpointMode"] = 'task_exit'


2. ``periodic``: The periodic mode allows the user to specify the interval at which
   all task information pending checkpointing should be checkpointed.

   >>> config["globals"]["checkpointMode"] = 'periodic'
   >>> config["globals"]["checkpointPeriod"] = "01:00:00"

3. ``dfk_exit``: In this mode, task checkpointing is done when the DataFlowKernel is
   about to exit. This reduces the risk of losing task information and results from
   premature workflow termination from exceptions, terminate signals etc. However
   there's still some likelihood that information might be lost if the workflow is
   terminated abruptly (machine failure, SIGKILL etc)

   >>> config["globals"]["checkpointMode"] = 'dfk_exit'

4. Manual: Apart from these modes, it is also possible to manually initiate a checkpoint
   by calling ``DataFlowKernel.checkpoint()`` in the workflow code.

In all of the above methods the checkpoint is written out to the ``runinfo/RUN_ID/checkpoint/`` directory.

Example code for setting the checkpoint methods:

.. code-block:: python

    config = {
        "sites": [
            {"site": "Local_Threads",
             "auth": {"channel": None},
             "execution": {
                 "executor": "threads",
                 "provider": None,
                 "maxThreads": 2,
             }
            }],
        "globals": {"lazyErrors": True,
                    "memoize": True,
                    "checkpointMode": "dfk_exit"
        }
    }
    dfk = DataFlowKernel(config=config)



Example code of manually invoking checkpoint:

.. code-block:: python

    from parsl import *
    from parsl.configs.local import localThreads as config
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

To load the checkpoint from a previous run specify the runinfo/RUNID/checkpoint directory:

.. code-block:: python

    import os
    from parsl import *
    from parsl.configs.local import localThreads as config

    last_runid = sorted(os.listdir('runinfo/'))[-1]
    last_checkpoint = os.path.abspath('runinfo/{0}/checkpoint'.format(last_runid))

    dfk = DataFlowKernel(config=config,
                         checkpointFiles=[last_checkpoint])
