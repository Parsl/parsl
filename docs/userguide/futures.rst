.. _label-futures:

Futures
=======

When a Python function is invoked, the Python interpreter waits for the function to complete execution
and returns the results. When functions execute for a long period of time it may not be desirable to wait for completion, instead it is often preferable that the function executes asynchronously. Parsl provides such asynchronous behavior by returning a `future <https://en.wikipedia.org/wiki/Futures_and_promises>`_ in lieu of results.
A future is essentially an object that can be used track the status of an asynchronous task so that it may, in the future, be interrogated to find the status,
results, exceptions, etc. A future is a proxy for a result that may not yet be available.

Parsl provides two types of futures: AppFutures and DataFutures. While related, these two types of futures enable subtly different workflow patterns.

AppFutures
----------

AppFutures are the basic building block upon which Parsl scripts are built. Every invocation of a Parsl app returns an AppFuture which may be used to manage execution and control the workflow.
AppFutures are inherited from Python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.
AppFutures provide several key functionalities:

1. An AppFuture provides a way to check the current status of an app.

   .. code-block:: python

       @python_app
       def double(x):
             return x*2

       # doubled_x is an AppFuture
       doubled_x = double(10)

       # Check status of doubled_x, this will print True if the result is available, else false
       print(doubled_x.done())

2. An AppFuture provides a way to block and wait for the result of an app:

   .. code-block:: python

      @python_app
      def sleep_double(x):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # doubled_x is an AppFuture
      doubled_x = sleep_double(10)

      # The result() function will block until the app has completed
      print(doubled_x.result())

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


In addition to being able to capture exceptions raised by a specific app, Parsl also raises ``DependencyErrors`` when apps are unable to execute due to failures in prior dependent apps. That is, an app that is dependent on the successful completion of another app will fail with a dependency error if any of the apps on which it depends fail.


DataFutures
-----------

While AppFutures represent the execution of an asynchronous app, DataFutures represent the files an app produces. Parsl's dataflow model, in which data is passed from one app to another via files, requires such a construct to enable apps to validate the creation of required files and to subsequently resolve dependencies when input files are created. When invoking an app, Parsl requires that a list of output files be specified (using the outputs keyword argument). A DataFuture for each file is returned by the app when it is executed. Throughout execution of the app Parsl will monitor these files to 1) ensure they are created, and 2) pass them to any dependent apps. DataFutures are accessible through the ``outputs`` attribute of the AppFuture.
DataFutures are inherited from Python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.

The following code snippet shows how DataFutures are used:

.. code-block:: python

      # This app echoes the input string to the first file specified in the
      # outputs list
      @bash_app
      def echo(message, outputs=[]):
          return 'echo %s &> {outputs[0]}' % (message)

      # Call echo specifying the output file
      hello = echo('Hello World!', outputs=['hello1.txt'])

      # The AppFuture's outputs attribute is a list of DataFutures
      print(hello.outputs)

      # Print the contents of the output DataFuture when complete
      with open(hello.outputs[0].result().filepath, 'r') as f:
           print(f.read())

.. note::
      Adding `.filepath` is only needed on python 3.5. With python
      >= 3.6 the resulting file can maybe be passed to open directly.






