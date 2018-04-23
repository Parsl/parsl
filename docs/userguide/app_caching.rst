.. _label-appcaching:

AppCaching
----------

When developing a workflow, developers often run the same workflow
with incremental changes over and over. Often large fragments of
a workflow will not have changed, yet apps will be executed again, wasting
valuable developer time and computation resources. ``AppCaching``
solves this problem by storing results from apps that have completed
so that they can be re-used. By default caching is **not** enabled.
It must be explicitly enabled, either globally via the configuration, 
or on each app for which caching is desired. 

.. code-block:: python

   @app('bash', dfk, cache=True)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


AppCaching can be particularly useful when developing interactive workflows such as when
using a Jupyter notebook. In this case, cells containing apps are often re-executed as 
during development. Using AppCaching will ensure that only modified apps are re-executed.

Caveats
^^^^^^^

It is important to consider several important issues when using AppCaching:

- Determinism:  AppCaching is generally useful only when the apps are deterministic.
  If the outputs may be different for identical inputs, AppCaching will hide
  this non-deterministic behavior. For instance, caching an app that returns
  a random number will result in every invocation returning the same result.

- Timing: If several identical calls to a previously defined app are
  made for the first time, many instances of the app will be launched as no cached
  result is yet available. Once one such app completes and the result is cached
  all subsequent calls will return immediately with the cached result.

- Performance: If AppCaching is enabled, there is likely to be some performance
  overhead especially if a large number of short duration tasks are launched rapidly.

.. note::
   The performance penalty has not yet been quantified.


Configuration
^^^^^^^^^^^

AppCaching may be disabled globally in the configuration. If the
``appCache`` is set to ``False`` all AppCaching is disabled. 
By default the global ``appCache`` is **enabled**; however, AppCaching for each
app is disabled by default. Thus, users must explicitly enable AppCaching 
on each app.

AppCaching can be disabled globally in the config as follows:

    .. code-block:: python

       config = {
           "sites": [{ ... }],
           "globals": {
                "appCache": False # <-- Disable AppCaching globally
       }

       dfk = DataFlowKernel(config=config)
