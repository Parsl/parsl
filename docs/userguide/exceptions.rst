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
