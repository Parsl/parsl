AppCaching
----------

When developing a workflow, developers often run the same workflow
with incremental changes over and over. Often large fragments of
the workflow have not been changed yet are computed again, wasting
valuable developer time and computation resources. ``AppCaching``
solves this problem by caching results from apps that have completed
so that they can be re-used. By default individual apps are set to
``not`` cache, and must be enabled explicitly like :

.. code-block:: python

   @app('bash', dfk, cache=True)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


Caveats
^^^^^^^

Here are some important considerations before using AppCaching :

Jupyter
"""""""

AppCaching can be useful for interactive workflows such as when
developing on a Jupyter notebook where cells containing apps are often
rerun as partof the development flow.


Determinism
"""""""""""

AppCaching is generally useful only when the apps are deterministic.
If the outputs may be different for identical inputs, caching will hide
this non-deterministic behavior. For instance caching an app that returns
a random number will result in every invocation returning the same result.


Timing
""""""

If several identical calls to previously defined app hello are
made for the first time, several apps will be launched since no cached
result is available. Once one such app completes and the result is cached
all subsequent calls will return immediately with the cached result.


Performance
"""""""""""

If appCaching is enabled, some minor performance penalty will be seen
especially when thousands of subsecond tasks are launched rapidly.

.. note::
   The performance penalty has not yet been quantified.


Configuring
^^^^^^^^^^^

The ``appCache`` option in the config is the master switch, which if set
to ``False`` disables all appCaching. By default the global ``appCache``
is **enabled**, and appCaching is disabled for each app individually, which
can be enabled to pick and choose what apps are to be cached.

Disabling appCaching globally :

1. Disabling appCaching globally via config:

    .. code-block:: python

       config = {
           "sites": [{ ... }],
           "globals": {
                "appCache": False # <-- Disable appCaching globally
       }

       dfk = DataFlowKernel(config=config)

2. Disabling appCaching globally via option to DataFlowKernel:

    .. code-block:: python

       workers = ThreadPoolExecutor(max_workers=4)
       dfk = DataFlowKernel(executors=[workers], appCache=False)


Checkpointing
-------------

Checkpointing extends appCaching a step further and allows you to re-use
results from and across multiple workflows.

.. note::
   Checkpointing is *only* possible for apps which have appCaching enabled.

.. note::
   If appCaching is disabled in the ``config['globals']``, checkpointing will
   **not** work

Parsl follows an incremental checkpointing model, where each call to checkpoint,
will checkpoint all newly updated results since the last checkpoint. When loading
checkpoints, if entries with results from multiple functions (with identical hashes)
are encountered only the last entry read will be considered.

Checkpointing comes with the same caveats as AppCaching but with one key
difference, each checkpointing event is manually triggered by the user.
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

   for i in range(0, n):
       d[i] = slow_double(i)

   # Wait for the results
   [d[i].result() for i in range(0, n)]

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
