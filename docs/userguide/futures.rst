.. _label-futures:

Futures
=======

When an ordinary Python function is invoked in a Python program, the Python interpreter waits for the function to complete execution
before proceeding to the next statement. 
But if a function is expected to execute for a long period of time, it may be preferable not to wait for its completion but instead to proceed immediately with executing subsequent statements.
The function can then execute concurrently with that other computation.

Concurrency can be used to enhance performance when concurrent activities can execute on different cores or nodes,
as in the following code fragment, where we may save time by executing the two function calls at the same time.

   .. code-block:: python

       v1 = expensive_function(1)
       v2 = expensive_function(2)
       result = v1 + v2
     
However, concurrency also introduces a need for **synchronization**.
In the example, we do not want to sum `v1` and `v2` until the function calls have completed,
as the values of those two variables may not be valid before then.
A synchronization mechanism provides a way of blocking execution of one activity
(here, the statement `result = v1 + v2`) until other activities (here, the two calls to `expensive_function()`) have completed.

Parsl supports concurrency and synchronization as follows. 
Whenever a program calls a function that the programmer has defined to be a Parsl app (see :ref:`label-apps`),
Parsl starts the function executing in a separate thread or process (on the same or another computer), returns a `future <https://en.wikipedia.org/wiki/Futures_and_promises>`_ in lieu of that function's result(s), 
and continues immediately to the next statement in the Python program.
Furthermore, when a computation attempts to access the value of a future (using mechanisms that we describe below), 
Parsl blocks that computation until the future has a value. 

More generaally, a future is an object that can be used track the status of an asynchronous task. 
This object may, in the future, be interrogated to determine the task's status,
results, exceptions, etc. A future is a proxy for a result that may not yet be available.

Parsl provides two types of futures: AppFutures and DataFutures. While related, they enable subtly different workflow patterns.

AppFutures
----------

AppFutures are the basic building block upon which Parsl scripts are built. Every invocation of a Parsl app returns an AppFuture that may be used to monitor and manage the task's execution.
AppFutures are inherited from Python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.
They provide three key functionalities:

1. An AppFuture's ``result()`` function can be used to wait for an app to complete, and then access any result(s).
This function is blocking: it returns only when the app completes or fails. 
Thus the following code fragment implements essentially the behavior of the `expensive_function()` example above:
the addition executes only when the two `sleep_double()` function calls complete.

   .. code-block:: python

      @python_app
      def sleep_double(x):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # Start two concurrent sleep_double apps. doubled_x1 and doubled_x2 are AppFutures
      doubled_x1 = sleep_double(10)
      doubled_x2 = sleep_double(5)

      # The result() function will block until each of the corresponding app calls have completed
      print(doubled_x1.result() + doubled_x2.result())

2. An AppFuture's ``done()`` function can be used to check the status of an app, *without blocking*.
This function is non-blocking.

   .. code-block:: python

       @python_app
       def double(x):
             return x*2

       # doubled_x is an AppFuture
       doubled_x = double(10)

       # Check status of doubled_x, this will print True if the result is available, else False
       print(doubled_x.done())

3. An AppFuture provides a safe way to handle exceptions and errors while executing complex workflows.

   .. code-block:: python

      @python_app
      def bad_divide(x):
          return 6/x

      # Call bad divide with 0, to cause a divide by zero exception
      doubled_x = bad_divide(0)

      # Catch and handle the exception.
      try:
           doubled_x.result()
      except ZeroDivisionError as ze:
           print('Oops! You tried to divide by 0 ')
      except Exception as e:
           print('Oops! Something really bad happened')


In addition to being able to capture exceptions raised by a specific app, Parsl also raises ``DependencyErrors`` when apps are unable to execute due to failures in prior dependent apps. 
That is, an app that is dependent upon the successful completion of another app will fail with a dependency error if any of the apps on which it depends fail.


DataFutures
-----------

While an AppFuture represents the execution of an asynchronous app, a DataFuture represent a file that an app produces.
Parsl's dataflow model requires such a construct so that it can determine when other apps that are to consume a file produced by the app can start execution. 
When calling an app that produces files as outputs, Parsl requires that a list of output files be specified via the ``outputs`` keyword argument. A DataFuture is returned for each file by the app when it executes. 
As the app executes, Parsl monitors each file to 1) ensure it is created, and 2) pass it to any dependent app(s). The DataFutures thus produced by an app are accessible through the ``outputs`` attribute of the AppFuture.
DataFutures inherit from and extend Python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.

The following code snippet shows how DataFutures are used:

.. code-block:: python

      # This app echoes the input string to the first file specified in the
      # outputs list
      @bash_app
      def echo(message, outputs=[]):
          return 'echo {} &> {}'.format(message, outputs[0])

      # Call echo specifying the output file
      hello = echo('Hello World!', outputs=[File('hello1.txt')])

      # The AppFuture's outputs attribute is a list of DataFutures
      print(hello.outputs)

      # Print the contents of the output DataFuture when complete
      with open(hello.outputs[0].result().filepath, 'r') as f:
           print(f.read())

.. note::
      Adding `.filepath` is only needed on python 3.5. With python
      >= 3.6 the resulting file can be passed to `open` directly.






