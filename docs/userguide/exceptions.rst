.. _label-exceptions:

Exceptions
==========

Apps fail in remote settings due to a variety of reasons. To handle errors
Parsl captures, tracks, and provides functionality to appropriately respond to failures during
workflow execution. A specific executing instance of an App is called a **task**.
If a task is unable to complete execution within specified time limits and produce
the specified set of outputs it is considered to have failed.

Failures might occur to one or more of the following reasons:

1. Task exceed specified walltime.
2. Formatting error while formatting the command-line string in ``Bash Apps``
3. Task failed during execution
4. Task completed execution but failed to produce one or more of it's specified
   outputs.
5. The App failed to launch, for example if an input dependency is not met.


Since Parsl tasks are executed asynchronously, we are faced with the issue of
determining where to place exception handling code in the workflow.
In Parsl all exceptions are associated with the task futures. These exceptions are raised only when a result is called on the future
of a failed task. For example:

.. code-block:: python

      @App('python', dfk)
      def bad_divide(x):
          return 6/x

      # Call bad divide with 0, to cause a divide by zero exception
      doubled_x = bad_divide(0)

      # Here we can catch and handle the exception.
      try:
           doubled_x.result()
      except ZeroDivisionError as e:
           print('Oops! You tried to divide by 0 ')
      except Exception ase:
           print('Oops! Something really bad happened')







