.. _label-appcaching:

App caching
----------

There are many situations in which a program may be re-executed
over time. Often, large fragments of the program will not have changed 
and therefore, re-execution of apps will waste valuable time and 
computation resources. Parsl's app caching solves this problem by 
storing results from apps that have successfully completed
so that they can be re-used. 

App caching is enabled by setting the ``cache``
argument in the :func:`~parsl.app.app.python_app` or :func:`~parsl.app.app.bash_app` 
decorator to ``True`` (by default it is ``False``). 

.. code-block:: python

   @bash_app(cache=True)
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)
			
App caching can be globally disabled by setting ``app_cache=False``
in the :class:`~parsl.config.Config`.

App caching can be particularly useful when developing interactive programs such as when
using a Jupyter notebook. In this case, cells containing apps are often re-executed
during development. Using app caching will ensure that only modified apps are re-executed.


App equivalence 
^^^^^^^^^^^^^^^

Parsl determines app equivalence by storing the a hash
of the app function. Thus, any changes to the app code (e.g., 
its signature, its body, or even the docstring within the body)
will invalidate cached values. 
Further, Parsl does not traverse imported modules, and thus
changes to modules used by an app will not invalidate cached
values.


Invocation equivalence 
^^^^^^^^^^^^^^^^^^^^^^

Two app invocations are determined to be equivalent if their
input arguments are identical. This equivalence is determined by hashing the input
arguments, and comparing hashes. 

Of course, this approach can only be applied to data types for which a 
deterministic hash can be computed. 

By default Parsl can compute sensible hashes for basic data types:
str, int, float, None, as well as more some complex types:
functions, and dictionaries and lists containing hashable types.

Attempting to cache apps invoked with other, non-hashable, data types will 
lead to an exception at invocation.

Mechanisms to hash new types can be registered by a program by using the
`parsl.dataflow.memoization.id_for_memo` single dispatch function.


Ignoring arguments
^^^^^^^^^^^^^^^^^^

On occasion one may wish to ignore particular arguments when determining
app invocation equivalence. For example, when generating log file
names automatically based on time or run information. 
Parsl allows developers to list the arguments to be ignored
in the ``ignore_for_cache`` app decorator parameter:

.. code-block:: python

   @bash_app(cache=True, ignore_for_cache=['stdout'])
   def hello (msg, stdout=None):
       return 'echo {}'.format(msg)


Caveats
^^^^^^^

It is important to consider several important issues when using app caching:

- Determinism: App caching is generally useful only when the apps are deterministic.
  If the outputs may be different for identical inputs, app caching will obscure
  this non-deterministic behavior. For instance, caching an app that returns
  a random number will result in every invocation returning the same result.

- Timing: If several identical calls to an app are made concurrently having
  not yet cached a result, many instances of the app will be launched.
  Once one invocation completes and the result is cached
  all subsequent calls will return immediately with the cached result.

- Performance: If app caching is enabled, there may be some performance
  overhead especially if a large number of short duration tasks are launched rapidly.
  This overhead has not been quantified.
