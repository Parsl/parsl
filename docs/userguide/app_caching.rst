.. _label-appcaching:

AppCaching
----------

When developing a workflow, developers often run the same workflow
with incremental changes over and over. Often large fragments of
the workflow have not been changed yet are computed again, wasting
valuable developer time and computation resources. ``AppCaching``
solves this problem by caching results from apps that have completed
so that they can be re-used. By default individual apps are set to
``not`` cache, and must be enabled explicitly like:

.. code-block:: python

   @app('bash', dfk, cache=True)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


Caveats
^^^^^^^

Here are some important considerations before using AppCaching:

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

If AppCaching is enabled, some minor performance penalty will be seen
especially when thousands of subsecond tasks are launched rapidly.

.. note::
   The performance penalty has not yet been quantified.


Configuring
^^^^^^^^^^^

The ``appCache`` option in the config is the master switch, which if set
to ``False`` disables all AppCaching. By default the global ``appCache``
is **enabled**, and AppCaching is disabled for each app individually, which
can be enabled to pick and choose what apps are to be cached.

Disabling AppCaching globally :

1. Disabling AppCaching globally via config:

    .. code-block:: python

       config = {
           "sites": [{ ... }],
           "globals": {
                "appCache": False # <-- Disable AppCaching globally
       }

       dfk = DataFlowKernel(config=config)

2. Disabling AppCaching globally via option to DataFlowKernel:

    .. code-block:: python

       workers = ThreadPoolExecutor(max_workers=4)
       dfk = DataFlowKernel(executors=[workers], appCache=False)
