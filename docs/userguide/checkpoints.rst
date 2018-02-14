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

Checkpointing comes with the same caveats as AppCaching but with one key
difference: each checkpointing event is manually triggered by the user.
Checkpoints are loaded from checkpoint files at the start of the
DataFlowKernel and written out to checkpoint files only when the
``DataFlowKernel.checkpoint()`` function is invoked. The checkpoint is written
out to the ``runinfo/RUN_ID/checkpoint/`` directory.

Example code follows:

.. code-block:: python

   config = { .... } # Define the config
   dfk = DataFlowKernel(config=config)

   @App('python', dfk, cache=True)
   def slow_double(x, sleep_dur=1):
       import time
       time.sleep(sleep_dur)
       return x * 2

   N = 5  # Number of calls to slow_double
   d = [] # List to store the futures
   for i in range(0, N):
       d.append(slow_double(i))

   # Wait for the results
   [i.result() for i in d]

   cpt_dir = dfk.checkpoint()
   print(cpt_dir) # Prints the checkpoint dir

To load the checkpoint from a previous run specify the runinfo/RUNID directory:

.. code-block:: python

   import os

   last_runid = sorted(os.listdir('runinfo/'))[-1]
   last_checkpoint = os.path.abspath('runinfo/{0}'.format(last_runid))

   config = { ... }
   dfk = DataFlowKernel(config=config,
                        checkpointFiles=[last_checkpoint])
