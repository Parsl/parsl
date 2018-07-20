.. _label-exceptions:

Error handling
==============

In this section we will cover the various mechanisms Parsl provides to add resiliency
and robustness to workflows.

Exceptions
----------

Apps fail in remote settings for a variety of reasons. To handle errors
Parsl captures, tracks, and provides functionality to appropriately respond to failures during
workflow execution. A specific executing instance of an app is called a *task*.
If a task is unable to complete execution within specified time limits and produce
the specified set of outputs it is considered to have failed.

Failures might occur to one or more of the following reasons:

1. Task exceeded specified walltime.
2. Formatting error while formatting the command-line string in Bash apps
3. Task failed during execution
4. Task completed execution but failed to produce one or more of its specified
   outputs.
5. The app failed to launch, for example if an input dependency is not met.


Since Parsl tasks are executed asynchronously, we are faced with the issue of
determining where to place exception handling code in the workflow.
In Parsl all exceptions are associated with the task futures. These exceptions are raised only when a result is called on the future
of a failed task. For example:

.. code-block:: python

      @python_app
      def bad_divide(x):
          return 6 / x

      # Call bad divide with 0, to cause a divide by zero exception
      doubled_x = bad_divide(0)

      # Here we can catch and handle the exception.
      try:
           doubled_x.result()
      except ZeroDivisionError as e:
           print('Oops! You tried to divide by 0.')
      except Exception as e:
           print('Oops! Something really bad happened.')


Retries
-------

Retries are one of the simplest and most frequently used methods to add resiliency
to app failures. By retrying failed apps, transient failures (eg. machine failure,
network failure) and intermittent failures within applications can be addressed.
When ``retries`` are enabled (set to integer > 0), Parsl will automatically
re-launch applications that have failed, until the retry limit is reached.

By default ``retries = 0``. Retries can be enabled by setting ``retries`` in the
``Config`` object specified to Parsl.

Here is an example of setting retries via the config:

.. code-block:: python

   from parsl import load
   from parsl.tests.configs.local_threads import config
   config.retries = 2

   load(config)




Lazy fail
---------

.. warning::
   Due to a known bug (`issue#282 <https://github.com/Parsl/parsl/issues/282>`_),
   disabling lazy_errors with ``lazy_errors=False`` is **not** supported in Parsl 0.6.0.


While retries address resiliency at the level of apps, lazy failure adds
resiliency at the workflow level. When lazy failures are enabled, the workflow does
not halt as soon as it encounters a failure, but continues execution of every
app that is unaffected. Lazy failures is the default behavior in Parsl, with the
expectation that when running production workflows, individual app failures can be
deferred until the end of the workflow. During the development and testing of
workflows, failing immediately on any failure is often preferred and this behavior
is possible by setting ``lazy_errors=False``.


For example:

.. code-block:: python

    Here's a workflow graph, where
         (X)  is runnable,
         [X]  is completed,
         (X*) is failed.
         (!X) is dependency failed

      (A)           [A]           (A)
      / \           / \           / \
    (B) (C)       [B] (C*)      [B] (C*)
     |   |   =>    |   |   =>    |   |
    (D) (E)       (D) (E)       [D] (!E)
      \ /           \ /           \ /
      (F)           (F)           (!F)

      time ----->


Lazy errors can be disabled by setting `lazy_errors=False` in the :class:`parsl.config.Config`.
