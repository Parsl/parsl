.. _label-appcaching:

App caching
----------

When developing a workflow, developers often run the same workflow
with incremental changes over and over. Often large fragments of
a workflow will not have changed, yet apps will be re-executed, wasting
valuable developer time and computation resources. App caching
solves this problem by storing results from apps that have completed
so that they can be re-used. App caching can be enabled by setting the ``cache``
argument in the :func:`~parsl.app.app.python_app` or :func:`~parsl.app.app.bash_app` decorator to ``True`` (by default it is ``False``). App caching
can be globally disabled by setting ``app_cache=False``
in the :class:`~parsl.config.Config`.

.. code-block:: python

   @bash_app(cache=True)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


App caching can be particularly useful when developing interactive workflows such as when
using a Jupyter notebook. In this case, cells containing apps are often re-executed
during development. Using app caching will ensure that only modified apps are re-executed.


Inputs
^^^^^^

Two app invocations are treated as the same by the caching mechanism if their
inputs are the same. This sameness is determined by hashing the inputs, and
comparing hashes.

This only makes sense for some datatypes.

By default parsl knows how to compute sensible hashes for basic data types:
str, int, float, None, as well as more some more complex types:
functions, and dicts and lists containing hashable types.

Attempting to cache apps invoked with other, unknown, types will lead to an
exception at invocation.

Mechanisms to hash new types can be registered by a workflow by using the
parsl.dataflow.memoization.id_for_memo single dispatch function.


Ignoring some arguments
^^^^^^^^^^^^^^^^^^^^^^^

Some app invocation inputs can be ignored for the purposes of determining if
two invocations are the same. This can be useful when generating log file
names automatically based on time or run information. The names of keyword
arguments to ignore can be specified as an ``ignore_for_cache``
parameter to the decorator:

.. code-block:: python

   @bash_app(cache=True, ignore_for_cache=['stdout'])
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


Caveats
^^^^^^^

It is important to consider several important issues when using app caching:

- Determinism: App caching is generally useful only when the apps are deterministic.
  If the outputs may be different for identical inputs, app caching will hide
  this non-deterministic behavior. For instance, caching an app that returns
  a random number will result in every invocation returning the same result.

- Timing: If several identical calls to a previously defined app are
  made for the first time, many instances of the app will be launched as no cached
  result is yet available. Once one such app completes and the result is cached
  all subsequent calls will return immediately with the cached result.

- Performance: If app caching is enabled, there is likely to be some performance
  overhead especially if a large number of short duration tasks are launched rapidly.

.. note::
   The performance penalty has not yet been quantified.
