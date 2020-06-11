.. _label-futures:

Futures
=======

When an ordinary Python function is invoked in a Python program, the Python interpreter waits for the function to complete execution
before proceeding to the next statement. 
But if a function is expected to execute for a long period of time, it may be preferable not to wait for
its completion but instead to proceed immediately with executing subsequent statements.
The function can then execute concurrently with that other computation.

Concurrency can be used to enhance performance when independent activities
can execute on different cores or nodes in parallel. The following
code fragment demonstrates this idea, showing that overall execution time
may be reduced if the two function calls are executed concurrently. 

.. code-block:: python

    v1 = expensive_function(1)
    v2 = expensive_function(2)
    result = v1 + v2
     
However, concurrency also introduces a need for **synchronization**.
In the example, it is not possible to compute the sum of ``v1`` and ``v2`` 
until both function calls have completed.
Synchronization provides a way of blocking execution of one activity
(here, the statement ``result = v1 + v2``) until other activities 
(here, the two calls to ``expensive_function()``) have completed.

Parsl supports concurrency and synchronization as follows. 
Whenever a Parsl program calls a Parsl app (a function annotated with a Parsl
app decorator, see :ref:`label-apps`),
Parsl will create a new `task` and immediately return a 
`future <https://en.wikipedia.org/wiki/Futures_and_promises>`_ in lieu of that function's result(s). 
The program will then continue immediately to the next statement in the program.
At some point, for example when the task's dependencies are met and there
is available computing capacity, Parsl will execute the task. Upon
completion, Parsl will set the value of the future to contain the task's 
output. 

A future can be used to track the status of an asynchronous task. 
For example, after creation, the future may be interrogated to determine 
the task's status (e.g., running, failed, completed), access results, 
and capture exceptions. Further, futures may be used for synchronization, 
enabling the calling Python program to block until the future 
has completed execution. 

Parsl provides two types of futures: `AppFuture` and `DataFuture`. 
While related, they enable subtly different parallel patterns.

AppFutures
----------

AppFutures are the basic building block upon which Parsl programs are built. Every invocation of a Parsl app returns an AppFuture that may be used to monitor and manage the task's execution.
AppFutures are inherited from Python's `concurrent library <https://docs.python.org/3/library/concurrent.futures.html>`_.
They provide three key capabilities:

1. An AppFuture's ``result()`` function can be used to wait for an app to complete, and then access any result(s).
This function is blocking: it returns only when the app completes or fails. 
The following code fragment implements an example similar to the ``expensive_function()`` example above.
Here, the `sleep_double` app simply doubles the input value. The program invokes
the ``sleep_double`` app twice, and returns futures in place of results. The example
shows how the future's ``result()`` function can be used to wait for the results from the 
two ``sleep_double`` app invocations to be computed.

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
The following example shows that calling the future's ``done()`` function will not stop execution of the main Python program.

.. code-block:: python

    @python_app
    def double(x):
        return x*2

    # doubled_x is an AppFuture
    doubled_x = double(10)

     # Check status of doubled_x, this will print True if the result is available, else False
     print(doubled_x.done())

3. An AppFuture provides a safe way to handle exceptions and errors while asynchronously executing
apps. The example shows how exceptions can be captured in the same way as a standard Python program
when calling the future's ``result()`` function.

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

While an AppFuture represents the execution of an asynchronous app, 
a DataFuture represents a file to be produced by that app.
Parsl's dataflow model requires such a construct so that it can determine 
when dependent apps, apps that that are to consume a file produced by another app, 
can start execution. 

When calling an app that produces files as outputs, Parsl requires that a list of output files be specified (as a list of `File` objects passed in via the ``outputs`` keyword argument). Parsl will return a DataFuture for each output file as part AppFuture when the app is executed. 
These DataFutures are accessible in the AppFuture's ``outputs`` attribute.

Each DataFuture will complete when the App has finished executing,
and the corresponding file has been created (and if specified, staged out).

When a DataFuture is passed as an argument to a subsequent app invocation,
that subsequent app will not begin execution until the DataFuture is
completed. The input argument will then be replaced with an appropriate
File object.

The following code snippet shows how DataFutures are used. In this
example, the call to the echo Bash app specifies that the results
should be written to an output file ("hello1.txt"). The main
program inspects the status of the output file (via the future's
``outputs`` attribute) and then blocks waiting for the file to 
be created (``hello.outputs[0].result()``).

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
      Adding ``.filepath`` is only needed on Python 3.5. With Python
      >= 3.6 the resulting file can be passed to `open` directly.






