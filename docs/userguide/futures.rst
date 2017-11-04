.. _label-futures:

Futures
=======

When a python function is invoked, the python interpreter waits for the function to complete execution
and returns the results. With long running functions we may not want to wait for completion and may
want it to be asynchronous. So, in lieu of the results, we return a `future <https://en.wikipedia.org/wiki/Futures_and_promises>`_.
A future is essentially a token that allows us to track the status of an asynchronous task so that we may check the status,
results, exceptions, etc. A future is a proxy for a result that is not yet available.

In Parsl, we have two types of futures: AppFutures and DataFutures.

AppFutures
----------

AppFutures are inherited from the python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.
An AppFuture is returned by a call to any function that is decorated with Parsl's ``@App`` decorator.
There are four key functionalities that an AppFuture offers us :

1. An AppFuture allows us to check on the current status of launched parsl app and does not wait for it.

   .. code-block:: python

       @App('python', data_flow_kernel)
       def double(x):
             return x*2

       # doubled_x is an AppFuture
       doubled_x = double(10)

       # Check status of doubled_x, this will print True if the result is available, else false
       print(doubled_x.done())

2. The AppFuture allows us to block and wait for the result of the launched app:

   .. code-block:: python

      @App('python', data_flow_kernel)
      def sleep_double(x):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # doubled_x is an AppFuture
      doubled_x = sleep_double(10)

      # The result() waits till the sleep_double() app is done (2s wait) and then prints
      # the result from the app *10*
      print(doubled_x.result())

3. The AppFuture itself can be passed to another decorate app. The apps can will wait till all of the AppFutures that are inputs are resolved and then proceed with execution.

   .. code-block:: python

      @App('python', data_flow_kernel)
      def wait_sleep_double(x, fu_1, fu_2):
           import time
           time.sleep(2)   # Sleep for 2 seconds
           return x*2

      # Launch two apps, which will execute in parallel, since they don't have to
      # wait on any futures
      doubled_x = wait_sleep_double(10, None, None)
      doubled_y = wait_sleep_double(10, None, None)

      # The third depends on the first two :
      #    doubled_x   doubled_y     (2 s)
      #           \     /
      #           doublex_z          (2 s)
      doubled_z = wait_sleep_double(10, doubled_x, doubled_y)

      # doubled_z will be done in ~4s
      print(doubled_z.result())

4. The AppFuture provides a safe way to handle exceptions and errors while executing workflows that are deep and have a range of parallel processing apps.

   .. code-block:: python

      @App('python', data_flow_kernel)
      def bad_divide(x):
          return 6/x

      # Call bad divide with 0, to cause a divide by zero exception
      doubled_x = bad_divide(0)

      # Here we can catch and handle the exception.
      try:
           doubled_x.result()
      except ZeroDivisionError as e:
           print("Oops! You tried to divide by 0 ")
      except Exception ase:
           print("Oops! Something really bad happened")

   In addition to being able to capture the exceptions raised in the specific apps executions represented by AppFutures, Parsl also raises
   DependencyErrors when apps are unable to execute due to failures in their dependent apps.


DataFutures
-----------

Similar to AppFutures, DataFuture are inherited from the python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.
While AppFutures represent an asynchronous app task, the DataFuture represents the files it produces.
With Bash applications data flows from one app to another via files. Therefore Parsl needs to
keep track of the files produced by app and this is done by specifying the filenames of outputs as a
keyword argument to apps. A list of DataFutures each corresponding to the filenames in the ``outputs``
keyword args is available through the ``outputs`` attribute of the AppFuture.

Here's an example :

.. code-block:: python

      # This app echo's the string passed to it to the first file specified in the
      # outputs list
      @App('bash', data_flow_kernel)
      def echo(message, outputs=[]):
          return 'echo {0} &> {outputs[0]}'

      # This app *cat*s the contents of the first file in its inputs[] kwargs to
      # the first file in its outputs[] kwargs
      @App('bash', data_flow_kernel)
      def cat(inputs=[], outputs=[]):
          return 'cat {inputs[0]} > {outputs[0]}'

      #Call echo specifying the outputfile
      hello = echo("Hello World!", outputs=['hello1.txt'])

      # the outputs attribute of the AppFuture is a list of DataFutures
      print(hello.outputs)

      #This step *cat*s hello1.txt to hello2.txt
      hello2 = cat(inputs=[hello.outputs[0]], outputs=['hello2.txt'])

      # Wait for the cat app to complete before trying to read the output file
      hello2.result()

      with open(hello2.outputs[0].result(), 'r') as f:
           print(f.read())
   








