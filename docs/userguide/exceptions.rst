.. _label-exceptions:

Error handling
==============

Parsl provides various mechanisms to add resiliency and robustness to programs.

Exceptions
----------

Parsl is designed to capture, track, and handle various errors occurring
during execution, including those related to the program, apps, execution 
environment, and Parsl itself. 
It also provides functionality to appropriately respond to failures during
execution.

Failures might occur for various reasons:

1. A task failed during execution.
2. A task failed to launch, for example, because an input dependency was not met.
3. There was a formatting error while formatting the command-line string in Bash apps.
4. A task completed execution but failed to produce one or more of its specified
   outputs.
5. Task exceeded the specified walltime.

Since Parsl tasks are executed asynchronously and remotely, it can be difficult to determine
when errors have occurred and to appropriately handle them in a Parsl program.

For errors occurring in Python code, Parsl captures Python exceptions and returns
them to the main Parsl program. For non-Python errors, for example when a node
or worker fails, Parsl imposes a timeout, and considers a task to have failed
if it has not heard from the task by that timeout. Parsl also considers a task to have failed
if it does not meet the contract stated by the user during invocation, such as failing
to produce the stated output files.

Parsl communicates these errors by associating Python exceptions with task futures.
These exceptions are raised only when a result is called on the future
of a failed task. For example:

.. code-block:: python

      @python_app
      def bad_divide(x):
          return 6 / x

      # Call bad divide with 0, to cause a divide by zero exception
      doubled_x = bad_divide(0)

      # Catch and handle the exception.
      try:
           doubled_x.result()
      except ZeroDivisionError as e:
           print('Oops! You tried to divide by 0.')
      except Exception as e:
           print('Oops! Something really bad happened.')


Retries
-------

Often errors in distributed/parallel environments are transient. 
In these cases, retrying failed tasks can be a simple way 
of overcoming transient (e.g., machine failure,
network failure) and intermittent failures.
When ``retries`` are enabled (and set to an integer > 0), Parsl will automatically
re-launch tasks that have failed until the retry limit is reached. 
By default, retries are disabled and exceptions will be communicated
to the Parsl program.

The following example shows how the number of retries can be set to 2:

.. code-block:: python

   import parsl
   from parsl.configs.htex_local import config
   
   config.retries = 2

   parsl.load(config)

More specific retry handling can be specified using retry handlers, documented
below.


Lazy fail
---------

Parsl implements a lazy failure model through which a workload will continue
to execute in the case that some tasks fail. That is, the program will not
halt as soon as it encounters a failure, rather it will continue to execute
unaffected apps.

The following example shows how lazy failures affect execution. In this
case, task C fails and therefore tasks E and F that depend on results from
C cannot be executed; however, Parsl will continue to execute tasks B and D
as they are unaffected by task C's failure.

.. code-block::

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


Retry handlers
--------------

The basic parsl retry mechanism keeps a count of the number of times a task
has been (re)tried, and will continue retrying that task until the configured
retry limit is reached.

Retry handlers generalize this to allow more expressive retry handling:
parsl keeps a retry cost for a task, and the task will be retried until the
configured retry limit is reached. Instead of the cost being 1 for each
failure, user-supplied code can examine the failure and compute a custom
cost.

This allows user knowledge about failures to influence the retry mechanism:
an exception which is almost definitely a non-recoverable failure (for example,
due to bad parameters) can be given a high retry cost (so that it will not
be retried many times, or at all), and exceptions which are likely to be
transient (for example, where a worker node has died) can be given a low
retry cost so they will be retried many times.

A retry handler can be specified in the parsl configuration like this:


.. code-block:: python

     Config(
          retries=2,
          retry_handler=example_retry_handler
          )


``example_retry_handler`` should be a function defined by the user that will
compute the retry cost for a particular failure, given some information about
the failure.

For example, the following handler will give a cost of 1 to all exceptions,
except when a bash app exits with unix exitcode 9, in which case the cost will
be 100. This will have the effect that retries will happen as normal for most
errors, but the bash app can indicate that there is little point in retrying
by exiting with exitcode 9.

.. code-block:: python

     def example_retry_handler(exception, task_record):
          if isinstance(exception, BashExitFailure) and exception.exitcode == 9:
               return 100
          else
               return 1

The retry handler is given two parameters: the exception from execution, and
the parsl internal task_record. The task record contains details such as the
app name, parameters and executor.
