9. Error Handling
=================

Parsl is designed to handle errors that may occur when running parallel workflows. These errors can range from issues within your Python code to problems with the underlying infrastructure or external services.

Understanding Exceptions
------------------------

Parsl uses Python exceptions to signal errors. When an error occurs during the execution of a Parsl app, Parsl captures the exception and stores it in the corresponding `AppFuture`. You can then retrieve the exception using the `.exception()` method of the `AppFuture`.

.. code-block:: python

   from parsl import python_app

   @python_app
   def divide(x, y):
       return x / y

   future = divide(10, 0)

   try:
       result = future.result()
   except ZeroDivisionError as e:
       print(f"Error: {e}")

In this example, the `divide` app raises a `ZeroDivisionError` when called with `y=0`. Parsl captures this exception and stores it in the `future` object. When we call `future.result()`, the exception is raised again, and we can catch it using a `try-except` block.

Retries and Lazy Fail
---------------------

Parsl provides two mechanisms to help you deal with transient errors:

- **Retries**: You can configure Parsl to automatically retry failed tasks a certain number of times. This can be useful for handling intermittent errors, such as network glitches or temporary resource unavailability.
- **Lazy Fail**: Parsl's lazy fail model allows your workflow to continue executing even if some tasks fail. This means that tasks that are not dependent on the failed tasks can still complete successfully.

Implementing Retry Handlers
---------------------------

By default, Parsl retries tasks a fixed number of times. However, you can customize the retry behavior by implementing a retry handler. A retry handler is a function that takes an exception and a task record as input and returns a retry cost. The retry cost is a number that determines how many retries the task has left.

.. code-block:: python

   from parsl.app.errors import AppException

   def my_retry_handler(exception, task_record):
       if isinstance(exception, AppException):
           return 1  # Retry AppException once
       else:
           return 100  # Don't retry other exceptions

   config = Config(retries=2, retry_handler=my_retry_handler)

In this example, the `my_retry_handler` function retries `AppException` once and doesn't retry other exceptions.

Practical Example: Error Handling in Parsl Scripts
--------------------------------------------------

.. code-block:: python

   import parsl
   from parsl import python_app, Config
   from parsl.executors import ThreadPoolExecutor

   config = Config(executors=[ThreadPoolExecutor(max_threads=4)])
   parsl.load(config)

   @python_app
   def unreliable_task(x):
       import random
       if random.random() < 0.5:
           raise Exception("Random failure")
       return x * 2

   results = []
   for i in range(10):
       results.append(unreliable_task(i))
   for result in results:
       try:
           print(result.result())
       except Exception as e:
           print(f"Task failed: {e}")

This script defines an `unreliable_task` that has a 50% chance of failing. It then creates 10 instances of this task and runs them in parallel. The `for` loop at the end iterates over the results and prints them if they are successful, or prints an error message if they failed.
